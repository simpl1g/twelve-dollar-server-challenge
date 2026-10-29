# frozen_string_literal: true

require "async"
require "extralite"
require "json"
require "rbnacl"

# libsodium's HMAC-SHA256 (one-shot for the warm-up; keyed state, update and final for requests) and
# constant-time compare, bound through rbnacl's FFI layer. The raw functions return 0 on success.
module Sodium
  extend RbNaCl::Sodium

  sodium_function :hmac_sha256, :crypto_auth_hmacsha256, %i[pointer pointer ulong_long pointer]
  attach_function :crypto_auth_hmacsha256_statebytes, [], :size_t
  attach_function :crypto_auth_hmacsha256_init, %i[pointer pointer size_t], :int
  attach_function :crypto_auth_hmacsha256_update, %i[pointer pointer ulong_long], :int
  attach_function :crypto_auth_hmacsha256_final, %i[pointer pointer], :int
  attach_function :crypto_verify_32, %i[pointer pointer], :int
end

# One SQLite connection, owned by the single reactor thread. SQLite renders every response
# document, so Ruby only routes, verifies the JWT and hands the bytes to the HTTP layer.
#
# Group commit: a write only queues its statement and yields once, so every write that is ready in
# this reactor pass queues too. The first writer back from its yield runs the whole queue inside one
# BEGIN IMMEDIATE ... COMMIT, synchronously. No transaction is ever open across a fiber switch, so
# reads never see uncommitted rows and never wait, and each write answers after its COMMIT.
class Store
  POST_JSON = "json_object('id',p.id,'body',p.body,'created_at',p.created_at,'author',u.username," \
              "'like_count',(SELECT count(*) FROM likes l WHERE l.post_id=p.id))"
  # No ORDER BY inside group_concat: the subquery's ORDER BY ... LIMIT 20 runs as a co-routine that
  # hands rows over already sorted, so the aggregate's temp b-tree sorter was pure overhead.
  FEED = "SELECT '{\"posts\":['||coalesce(group_concat(doc,','),'')||']}' " \
         "FROM (SELECT #{POST_JSON} AS doc FROM posts p JOIN users u ON u.id=p.user_id " \
         "ORDER BY p.created_at DESC,p.id DESC LIMIT 20)"
  # The sorted text: compared against FEED once at boot, and used instead if the plans ever differ.
  FEED_SORTED = "SELECT '{\"posts\":['||coalesce(group_concat(doc,',' ORDER BY created_at DESC,id DESC),'')||']}' " \
                "FROM (SELECT #{POST_JSON} AS doc,p.created_at,p.id FROM posts p JOIN users u ON u.id=p.user_id " \
                "ORDER BY p.created_at DESC,p.id DESC LIMIT 20)"
  # `+?`, not `?`: with the join, SQLite's constant propagation makes the plan depend on the bound
  # id, so every new id would recompile the statement (twice the cost of running it).
  POST = "SELECT '{\"post\":'||#{POST_JSON}||'}' FROM posts p JOIN users u ON u.id=p.user_id WHERE p.id=+?"
  CREATE = "INSERT INTO posts(user_id,body) VALUES(?1,?2) RETURNING '{\"post\":'||json_object('id',id," \
           "'body',body,'created_at',created_at,'author',?3,'like_count',0)||'}'"
  # The foreign key on likes.post_id (PRAGMA foreign_keys=ON) rejects a like of a missing post.
  LIKE = "INSERT INTO likes(user_id,post_id) VALUES(?1,?2) ON CONFLICT(user_id,post_id) DO NOTHING"
  EXISTS = "SELECT 1 FROM posts WHERE id=?"
  FK_FAILED = "FOREIGN KEY constraint failed"

  def initialize(path)
    @db = Extralite::Database.new(path)
    @db.gvl_release_threshold = -1 # one thread: never release the GVL around queries
    # locking_mode=EXCLUSIVE first, so the WAL index lives in heap memory rather than the -shm file.
    @db.execute("PRAGMA locking_mode=EXCLUSIVE")
    @db.execute("PRAGMA journal_mode=WAL")
    @db.execute("PRAGMA synchronous=NORMAL")
    @db.execute("PRAGMA foreign_keys=ON")
    @db.execute("PRAGMA wal_autocheckpoint=0") # checkpoints run on a timer, off the commit path
    # No mmap: a mapped page is re-parsed and looked up in the WAL index on every acquisition,
    # while a page-cache hit keeps the parsed page. The ~40 MB working set fits the 64 MiB cache.
    @db.execute("PRAGMA mmap_size=0")
    @db.execute("PRAGMA cache_size=-65536")
    @db.execute("PRAGMA temp_store=MEMORY")
    @feed = @db.prepare_splat(feed_sql)
    @post = @db.prepare_splat(POST)
    @create = @db.prepare_splat(CREATE)
    @like = @db.prepare_splat(LIKE)
    @exists = @db.prepare_splat(EXISTS)
    @ping = @db.prepare_splat("SELECT 1")
    @begin = @db.prepare("BEGIN IMMEDIATE")
    @commit = @db.prepare("COMMIT")
    @rollback = @db.prepare("ROLLBACK")
    @pending = [] # [operation, slot] pairs waiting for the next flush
    @commits = 0
  end

  attr_reader :commits # group commits so far, for the pool diagnostics line

  # Every statement is reset as soon as its row is taken: a statement left mid-step keeps its read
  # transaction open, and while it is open no checkpoint can run.
  def feed
    document = @feed.reset.next
    @feed.reset
    document
  end

  def post(id)
    document = @post.bind(id).next # Query#bind resets the statement itself
    @post.reset
    document
  end

  def create(user_id, username, body)
    enqueue([:create, user_id, body, username])
  end

  # Returns :liked, :already_liked or :missing.
  def like(user_id, post_id)
    enqueue([:like, user_id, post_id])
  end

  def ping
    ok = @ping.reset.next
    @ping.reset
    ok
  end

  # Called every second. A flush never spans a fiber switch, so no write transaction is open here.
  def checkpoint
    @db.execute("PRAGMA wal_checkpoint(PASSIVE)")
  rescue Extralite::Error => error
    warn "checkpoint failed: #{error.message}" # retried on the next tick
  end

  private

  # FEED relies on co-routine delivery order (pinned SQLite 3.54.0), not on an ORDER BY contract.
  # Checked once at boot, both documents discarded; any difference falls back to the sorted text.
  def feed_sql
    if @db.query("EXPLAIN QUERY PLAN #{FEED}").any? { |row| row.values.join.include?("TEMP B-TREE") }
      warn "feed plan uses a sorter: using sorted FEED"
      FEED_SORTED
    elsif @db.query_single_splat(FEED) != @db.query_single_splat(FEED_SORTED)
      warn "feed plan drift: using sorted FEED"
      FEED_SORTED
    else
      FEED
    end
  end

  # Queues the operation and yields once, so every write that is ready in this pass queues too. The
  # first writer to resume flushes the whole queue; the others find their slot already frozen.
  def enqueue(op)
    slot = [] # [result] or [nil, error], frozen by the flush that ran it
    @pending << [op, slot]
    Fiber.scheduler&.yield # nil before the reactor starts (warm-up): then the flush runs inline
    flush unless slot.frozen?
    raise slot[1] if slot[1]

    slot[0] or raise "write lost: the flush did not run the queued operation"
  end

  # One synchronous transaction for every queued operation; nothing in here may switch fibers (no
  # logging, no sleeps). A failing statement fails only its own request, a failing BEGIN or COMMIT
  # fails them all, and the flush never ends with the transaction active or a slot left unfrozen.
  def flush
    ops = @pending
    @pending = []
    return if ops.empty?

    @begin.execute
    ops.each do |op, slot|
      slot[0] = run_op(op)
    rescue StandardError => error
      slot[1] = error
    end
    @commit.execute
    @commits += 1
  rescue StandardError => error
    ops.each do |_, slot|
      slot[0] = nil
      slot[1] ||= error
    end
  ensure
    rollback if @db.transaction_active?
    ops.each { |_, slot| slot.freeze }
  end

  def run_op(op)
    if op[0] == :create
      document = @create.bind(op[1], op[2], op[3]).next
      @create.reset
      document
    else
      insert_like(op[1], op[2])
    end
  end

  # A constraint failure means a missing post or a missing user; only then is the post looked up,
  # so a missing post answers 404 and a missing user stays an error.
  def insert_like(user_id, post_id)
    @like.execute(user_id, post_id) == 1 ? :liked : :already_liked
  rescue Extralite::Error => error
    raise error unless error.message.start_with?(FK_FAILED)

    exists = @exists.reset.bind(post_id).next
    @exists.reset
    exists ? raise(error) : :missing
  end

  # Must not raise: the transaction may not outlive the flush, whatever failed.
  def rollback
    @rollback.execute
  rescue StandardError
    nil
  end
end

# The API: routes a parsed request and returns [status, json_document]. Transport-free.
class App
  HS256_HEADER = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" # {"alg":"HS256","typ":"JWT"}
  NOT_BASE64URL = /[+\/=]/ # the strict decoder rejects every other byte outside the alphabet
  POSTS_PREFIX = "/posts/"
  LIKE_SUFFIX = "/like"
  POSITIVE_ID = /\A[0-9]+\z/
  MAX_ID = 9_223_372_036_854_775_807
  # JavaScript's String#trim set, so trimming matches the reference implementations.
  SPACE = "\u0009-\u000d    -     　﻿"
  TRIM = /\A[#{SPACE}]+|[#{SPACE}]+\z/

  NOT_FOUND = '{"error":"not found"}'
  INVALID_ID = '{"error":"invalid post id"}'
  POST_NOT_FOUND = '{"error":"post not found"}'
  MISSING_TOKEN = '{"error":"missing bearer token"}'
  INVALID_TOKEN = '{"error":"invalid or expired token"}'
  INVALID_PAYLOAD = '{"error":"invalid token payload"}'
  MALFORMED = '{"error":"malformed JSON body"}'
  BODY_REQUIRED = '{"error":"body is required"}'
  BODY_TOO_LONG = '{"error":"body must be at most 500 characters"}'
  INTERNAL = '{"error":"internal server error"}'
  # Parser options are resolved once here, not on every parse.
  CODER = JSON::Coder.new
  EMPTY = ""

  # Thrown with [status, document] to answer early.
  class Halt < StandardError
    attr_reader :status, :document

    def initialize(status, document)
      super()
      @status = status
      @document = document
    end
  end

  def initialize(store, secret)
    @store = store
    @key = secret.b.ljust(32, "\0") # HMAC zero-pads keys up to the block size: same MAC
    raise ArgumentError, "JWT_SECRET longer than 32 bytes is not supported" if @key.bytesize > 32

    # The keyed HMAC state (the secret's inner and outer pads) is computed once and every
    # verification starts from a copy of it. It derives from JWT_SECRET alone: no token is kept.
    size = Sodium.crypto_auth_hmacsha256_statebytes
    template = FFI::MemoryPointer.new(size)
    raise "crypto_auth_hmacsha256_init failed" unless Sodium.crypto_auth_hmacsha256_init(template, @key, @key.bytesize).zero?

    @hmac_template = template.get_bytes(0, size)
    @hmac_state = FFI::MemoryPointer.new(size)
    @mac = FFI::MemoryPointer.new(32)
    @started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  # `path` excludes the query string; `authorization` is the raw header value or nil.
  def call(method, path, authorization, body)
    if method == "GET"
      return [200, @store.feed] if path == "/feed"
      return health if path == "/health"
    elsif method == "POST" && path == "/posts"
      return create(authorization, body)
    end

    # /posts/:id and /posts/:id/like
    if path.start_with?(POSTS_PREFIX)
      liking = path.bytesize >= POSTS_PREFIX.bytesize + LIKE_SUFFIX.bytesize && path.end_with?(LIKE_SUFFIX)
      last = liking ? path.bytesize - LIKE_SUFFIX.bytesize : path.bytesize
      unless path.index("/", POSTS_PREFIX.bytesize)&.< last
        if method == "GET" && !liking
          id = post_id(path.byteslice(POSTS_PREFIX.bytesize, last - POSTS_PREFIX.bytesize))
          document = @store.post(id) or raise Halt.new(404, POST_NOT_FOUND)
          return [200, document]
        elsif method == "POST" && liking
          user_id, = authenticate(authorization)
          return like(user_id, post_id(path.byteslice(POSTS_PREFIX.bytesize, last - POSTS_PREFIX.bytesize)))
        end
      end
    end
    [404, NOT_FOUND]
  rescue Halt => halt
    [halt.status, halt.document]
  rescue StandardError => error
    Console.error(self, error)
    [500, INTERNAL]
  end

  private

  def health
    @store.ping
    uptime = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - @started).to_i
    [200, %({"status":"ok","db":"ok","uptime_s":#{uptime}})]
  rescue Extralite::Error => error
    [503, JSON.generate({ status: "degraded", db: "unreachable", error: error.message })]
  end

  def create(authorization, raw)
    user_id, username = authenticate(authorization)
    raw = raw ? raw.force_encoding(Encoding::UTF_8) : EMPTY
    raise Halt.new(400, MALFORMED) unless raw.valid_encoding? # the parser does not check UTF-8 itself

    payload = begin
      CODER.load(raw)
    rescue JSON::ParserError
      raise Halt.new(400, MALFORMED)
    end
    body = payload["body"] if payload.is_a?(Hash)
    raise Halt.new(400, BODY_REQUIRED) unless body.is_a?(String)

    # Printable ASCII at both ends proves there is nothing to trim: every character in SPACE is
    # ASCII below 0x21 or a multibyte sequence (first byte >= 0xC2, last byte 0x80-0xBF).
    first = body.getbyte(0)
    last = body.getbyte(-1)
    body = body.gsub(TRIM, "") unless first && first.between?(0x21, 0x7E) && last.between?(0x21, 0x7E)
    raise Halt.new(400, BODY_REQUIRED) if body.empty?
    raise Halt.new(400, BODY_TOO_LONG) if body.length > 500

    [201, @store.create(user_id, username, body)]
  end

  def like(user_id, id)
    case @store.like(user_id, id)
    when :liked then [201, %({"liked":true,"already_liked":false,"post_id":#{id}})]
    when :already_liked then [200, %({"liked":true,"already_liked":true,"post_id":#{id}})]
    else [404, POST_NOT_FOUND]
    end
  end

  def post_id(text)
    raise Halt.new(400, INVALID_ID) unless POSITIVE_ID.match?(text)

    id = text.to_i
    raise Halt.new(400, INVALID_ID) if id.zero?
    raise Halt.new(404, POST_NOT_FOUND) if id > MAX_ID

    id
  end

  # Verifies the HS256 signature and expiry on every request. Returns [user_id, username].
  def authenticate(header)
    raise Halt.new(401, MISSING_TOKEN) unless header&.start_with?("Bearer ")

    first = header.byteindex(".", 7)
    last = header.byterindex(".")
    raise Halt.new(401, INVALID_TOKEN) unless first && first != last && header.bytesize - last - 1 == 43
    raise Halt.new(401, INVALID_TOKEN) if header.match?(NOT_BASE64URL, 7)

    signing_input = header.byteslice(7, last - 7)
    @hmac_state.put_bytes(0, @hmac_template)
    Sodium.crypto_auth_hmacsha256_update(@hmac_state, signing_input, signing_input.bytesize)
    Sodium.crypto_auth_hmacsha256_final(@hmac_state, @mac)
    signature = decode64(header.byteslice(last + 1, 43))
    raise Halt.new(401, INVALID_TOKEN) unless signature.bytesize == 32 && Sodium.crypto_verify_32(@mac, signature).zero?

    head = header.byteslice(7, first - 7)
    unless head == HS256_HEADER
      fields = parse_segment(head)
      raise Halt.new(401, INVALID_TOKEN) unless fields.is_a?(Hash) && fields["alg"] == "HS256"
    end

    claims = parse_segment(header.byteslice(first + 1, last - first - 1))
    raise Halt.new(401, INVALID_TOKEN) unless claims.is_a?(Hash)

    now = Process.clock_gettime(Process::CLOCK_REALTIME)
    exp = claims["exp"]
    raise Halt.new(401, INVALID_TOKEN) unless exp.is_a?(Numeric) && exp > now

    nbf = claims["nbf"]
    raise Halt.new(401, INVALID_TOKEN) if nbf && !(nbf.is_a?(Numeric) && nbf <= now)

    sub = claims["sub"]
    username = claims["username"]
    unless sub.is_a?(String) && POSITIVE_ID.match?(sub) && username.is_a?(String) &&
           (user_id = sub.to_i).positive? && user_id <= MAX_ID
      raise Halt.new(401, INVALID_PAYLOAD)
    end

    [user_id, username]
  end

  def parse_segment(segment)
    CODER.load(decode64(segment)) # a BINARY source is read as UTF-8
  rescue JSON::ParserError, ArgumentError, EncodingError
    raise Halt.new(401, INVALID_TOKEN)
  end

  # Strict base64url: the padding follows from the length (1 mod 4 is never valid) and `m0` rejects
  # any byte outside the alphabet, misplaced padding and non-zero trailing bits.
  def decode64(segment)
    padded = segment.tr("-_", "+/")
    case segment.bytesize % 4
    when 2 then padded << "=="
    when 3 then padded << "="
    when 1 then raise Halt.new(401, INVALID_TOKEN)
    end
    padded.unpack1("m0")
  rescue ArgumentError
    raise Halt.new(401, INVALID_TOKEN)
  end
end
