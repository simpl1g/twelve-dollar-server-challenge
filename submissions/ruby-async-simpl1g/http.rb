# frozen_string_literal: true

require "socket"
require_relative "epoll"

# A small HTTP/1.1 server on the Async reactor (io-event: io_uring or epoll), specialised for this API:
# it parses the request line and the four headers the API needs, and writes each response with one
# write. Requests are handled in order per connection (pipelining works); bodies use Content-Length.
#
# Idle keep-alive connections own no fiber: they sit in a kernel epoll set (edge-triggered, so a
# connection is registered once and never re-armed). One Async task waits for that set to become
# readable through the reactor, and a pool of worker fibers serves the connections that are ready.
# A fiber per connection would keep the stack pages each one touched while serving (deep SQLite
# calls), which costs ~70 KB per idle connection.
#
# Idle workers wait on a LIFO stack and are handed a descriptor directly: the dispatcher pops the
# most recently parked worker (its stack is still cache-warm) and pushes its fiber onto the
# reactor's ready queue; the worker parks again by transferring back to the reactor. The pool
# grows while every worker is busy and a timer trims idle workers beyond WORKERS, so a burst does
# not leave hundreds of suspended fibers for every minor GC to mark.
#
# Close to the descriptor limit, new connections are shed instead: a worker reads their first
# request directly, answers it with `Connection: close` and closes them, so they never enter epoll.
class HTTPServer
  READ_SIZE = 8192
  MAX_HEAD = 16 * 1024
  MAX_BODY = 1024 * 1024
  WORKERS = 64 # resident workers; idle workers beyond this are trimmed once a second
  MAX_WORKERS = 4096 # the pool grows past WORKERS only if every worker is busy
  TRIM_STEP = 32 # at most this many idle workers exit per trim, so a burst's pool drains gradually
  MAX_EVENTS = 1024
  MAX_SHED = 2048 # shed connections that may wait for their first request at once
  SHED_TIMEOUT = 10 # bounds each read or write wait on a shed connection, not the connection as a whole

  REASONS = {
    200 => "OK", 201 => "Created", 400 => "Bad Request", 401 => "Unauthorized", 404 => "Not Found",
    411 => "Length Required", 413 => "Content Too Large", 431 => "Request Header Fields Too Large",
    500 => "Internal Server Error", 503 => "Service Unavailable"
  }.freeze
  # Response heads up to "content-length: "; the length, a blank line and the body follow.
  def self.heads(extra)
    REASONS.to_h { |code, reason| [code, "HTTP/1.1 #{code} #{reason}\r\n#{extra}content-type: application/json\r\ncontent-length: ".freeze] }.freeze
  end
  KEEP = heads("")
  KEEP_10 = heads("connection: keep-alive\r\n") # HTTP/1.0 clients that asked for keep-alive
  CLOSE = heads("connection: close\r\n")
  END_HEAD = "\r\n\r\n"
  CRLF = "\r\n"
  COLON = ":"
  # The only request headers the API needs. `drain` walks the header lines byte by byte and tells
  # the names apart by length and first letter before it slices anything, so a GET with Host,
  # User-Agent and Accept-Encoding allocates nothing for its headers.
  CONTENT_LENGTH = "content-length"
  TRANSFER_ENCODING = "transfer-encoding"
  CONNECTION = "connection"
  AUTHORIZATION = "authorization"
  DIGITS = /\A[0-9]+\z/
  CLOSE_TOKEN = /\bclose\b/i
  KEEP_ALIVE_TOKEN = /\bkeep-alive\b/i
  HTTP_11 = "HTTP/1.1"
  HTTP_10 = "HTTP/1.0"
  EMPTY = ""

  def initialize(app, host, port, connection_budget: nil)
    @app = app
    @server = TCPServer.new(host, port)
    @server.listen(65_535) # the kernel caps this at net.core.somaxconn
    # Accepted sockets inherit TCP_NODELAY from the listener on Linux: no setsockopt per connection.
    @server.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
    soft = Process.getrlimit(:NOFILE).first
    # Last-resort guard: only close to the descriptor limit do responses carry `Connection: close`.
    @budget = connection_budget || [soft - 256, soft * 9 / 10].max
    # Within 192 descriptors of the budget, new connections are shed: served once, with
    # `Connection: close`, and never registered in epoll. Served incumbents stay keep-alive up to
    # the budget itself.
    @shed_from = @budget - 192
    @open = 0
    @shed_in_flight = 0
  end

  attr_reader :open

  # A pooled fiber and the descriptor the dispatcher handed it (nil when it is told to exit).
  Worker = Struct.new(:fiber, :fd, :io_buffer)

  # Called once a second: evicts the longest-idle workers (the bottom of the stack) beyond WORKERS,
  # at most TRIM_STEP per call. An evicted worker is woken with no descriptor and exits.
  def trim_workers
    surplus = @idle.size - WORKERS
    return if surplus <= 0

    [surplus, TRIM_STEP].min.times do
      worker = @idle.shift
      worker.fd = nil
      @scheduler.push(worker.fiber)
    end
  end

  def stats
    "workers=#{@workers} idle=#{@idle.size} backlog=#{@backlog.size} open=#{@open}"
  end

  def run(task)
    @task = task
    @scheduler = Fiber.scheduler
    @epfd = Epoll.create
    @event = Epoll.event(Epoll::IN | Epoll::RDHUP | Epoll::ET)
    @sockets = [] # by descriptor
    @state = [] # by descriptor: IDLE, BUSY (queued or being served), AGAIN (busy, and new data came) or SHED
    @partial = {} # descriptor => bytes of a request that hasn't fully arrived
    @idle = [] # LIFO stack of parked Workers: the most recently parked one serves next
    @backlog = [] # ready descriptors that found no idle worker while the pool is at MAX_WORKERS
    @workers = 0
    WORKERS.times { add_worker }
    task.async { dispatch }
    task.async { accept_loop }
  end

  # Runs each request through the parser and the app `times` times over a socket pair, before the
  # reactor starts, so YJIT has compiled the whole request path when the first client arrives.
  def warm(requests, times)
    near, far = UNIXSocket.pair
    times.times do
      requests.each do |request|
        drain(near, request.b)
        far.read_nonblock(1 << 20, exception: false)
      end
    end
    near.close
    far.close
  end

  private

  def accept_loop
    loop do
      socket = @server.accept_nonblock(exception: false)
      if socket == :wait_readable
        @server.wait_readable
        next
      end
      @open += 1
      fd = socket.fileno
      @sockets[fd] = socket
      if @open > @shed_from && @shed_in_flight < MAX_SHED
        # Shed: a worker reads its first request directly; IO#timeout bounds that wait and the write.
        socket.timeout = SHED_TIMEOUT
        @state[fd] = SHED
        @shed_in_flight += 1
        worker = @idle.pop
        if !worker && @workers < MAX_WORKERS
          add_worker # runs until it parks, so it is on @idle now; the nil check below is the guard
          worker = @idle.pop
        end
        if worker
          worker.fd = fd
          @scheduler.push(worker.fiber)
        else
          @backlog.push(fd)
        end
      else
        @state[fd] = IDLE
        Epoll.control(@epfd, Epoll::CTL_ADD, fd, @event)
      end
    rescue Errno::EMFILE, Errno::ENFILE, Errno::ENOBUFS, Errno::ENOMEM
      sleep 0.01 # out of descriptors: let responses retire connections, then accept again
    rescue Errno::ECONNABORTED, Errno::EPROTO
      next
    end
  end

  # --- connections ----------------------------------------------------------------------

  IDLE = 0
  BUSY = 1
  AGAIN = 2
  SHED = 3 # not in the epoll set: served once by a worker, then closed

  # Waits (through the reactor) for the epoll set to have ready connections and hands each one to
  # an idle worker (growing the pool when none is idle), all in the same pass so their writes still
  # join one group commit. A connection already handed out is only flagged: one worker at a time.
  # A stale event for a reused descriptor (closed, or now SHED) matches no state and is ignored.
  def dispatch
    poller = IO.for_fd(@epfd, autoclose: false)
    events = Epoll.events(MAX_EVENTS)
    loop do
      poller.wait_readable
      Epoll.ready(@epfd, events, MAX_EVENTS) do |fd|
        case @state[fd]
        when IDLE
          @state[fd] = BUSY
          worker = @idle.pop
          if !worker && @workers < MAX_WORKERS
            add_worker # runs until it parks, so it is on @idle now; the nil check below is the guard
            worker = @idle.pop
          end
          if worker
            worker.fd = fd
            @scheduler.push(worker.fiber)
          else
            @backlog.push(fd)
          end
        when BUSY
          @state[fd] = AGAIN
        end
      end
    end
  end

  def add_worker
    @workers += 1
    @task.async { work }
  end

  # Serves backlogged descriptors first, otherwise parks on @idle until dispatch sets the worker's
  # fd and pushes its fiber. Woken with no fd (by trim_workers), the worker leaves the pool.
  def work
    worker = Worker.new(Fiber.current, nil, IO::Buffer.new(READ_SIZE))
    loop do
      fd = @backlog.shift
      unless fd
        worker.fd = nil
        @idle.push(worker)
        @scheduler.transfer
        fd = worker.fd or break
      end
      begin
        service(fd, worker.io_buffer)
      rescue StandardError => error
        Console.error(self, error)
      end
    end
    @workers -= 1
  end

  # Edge-triggered: a read shorter than READ_SIZE emptied the socket, and anything that arrives
  # after it raises a new edge, which `dispatch` turns into AGAIN while this worker still holds fd.
  #
  # The first read goes through the scheduler, so with io_uring the reads of every connection that
  # became ready in a reactor pass are submitted together. That read waits for data, so it is only
  # used where data is expected: a connection whose new data was already read in an earlier pass
  # (a stale edge) would hold this worker until its client sends again, which is why the pool can
  # grow. Re-reads after AGAIN, and reads past a full buffer, don't wait.
  #
  # A shed connection comes straight from `accept`, not from epoll: the same first read picks up
  # its request when it arrives, its one response closes it, and nothing else can wake it.
  def service(fd, io_buffer)
    socket = @sockets[fd] or return
    shed = @state[fd] == SHED
    wait = true
    loop do
      @state[fd] = BUSY unless shed
      buffer = @partial.delete(fd)
      loop do
        data = if wait
                 wait = false
                 length = Fiber.scheduler.io_read(socket, io_buffer, 1, 0)
                 io_buffer.get_string(0, length) if length.positive?
               else
                 socket.read_nonblock(READ_SIZE, exception: false)
               end
        break if data == :wait_readable
        return disconnect(fd, socket) unless data

        buffer = buffer ? buffer << data : data
        break if data.bytesize < READ_SIZE
      end

      if buffer
        rest = drain(socket, buffer, shed) or return disconnect(fd, socket)
        @partial[fd] = rest unless rest.empty?
      end
      if shed # not in epoll: wait here for the rest of the request (IO#timeout bounds the wait)
        socket.wait_readable
        next
      end
      next if @state[fd] == AGAIN

      @state[fd] = IDLE
      return
    end
  rescue IOError, SystemCallError
    disconnect(fd, socket)
  end

  def disconnect(fd, socket)
    @shed_in_flight -= 1 if @state[fd] == SHED
    @sockets[fd] = nil
    @state[fd] = nil
    @partial.delete(fd)
    @open -= 1
    socket.close unless socket.closed?
  end

  # --- HTTP/1.1 ---------------------------------------------------------------------------

  # Answers every complete request in `buffer`, in order. Returns the unconsumed tail (possibly
  # empty), or nil when the connection must be closed.
  def drain(socket, buffer, shed = false)
    position = 0
    size = buffer.bytesize
    while position < size
      head_end = buffer.byteindex(END_HEAD, position)
      unless head_end
        return reject(socket, 431) if size - position > MAX_HEAD

        break
      end

      line_end = buffer.byteindex(CRLF, position)
      space = buffer.byteindex(" ", position)
      space2 = space && buffer.byteindex(" ", space + 1)
      return reject(socket, 400) unless space2 && space > position && space2 < line_end

      version = buffer.byteslice(space2 + 1, line_end - space2 - 1)
      http10 = version == HTTP_10
      return reject(socket, 400) unless http10 || version == HTTP_11

      method = buffer.byteslice(position, space - position)
      query = buffer.byteindex("?", space + 1)
      path_end = query && query < space2 ? query : space2
      path = buffer.byteslice(space + 1, path_end - space - 1)

      length = 0
      authorization = nil
      close = http10
      pos = line_end + 2
      colon = -1
      while pos < head_end
        eol = buffer.byteindex(CRLF, pos)
        # The colon found is reused until the lines catch up with it; when none is left, head_end
        # stands in, so one drain scans the buffer for a colon at most once.
        colon = buffer.byteindex(COLON, pos) || head_end if colon < pos
        if colon < eol
          name_end = colon
          name_end -= 1 while name_end > pos && ((b = buffer.getbyte(name_end - 1)) == 32 || b == 9)
          name_len = name_end - pos
          letter = buffer.getbyte(pos) | 0x20
          if (name_len == 14 && letter == 0x63) || (name_len == 17 && letter == 0x74) ||
             (name_len == 10 && letter == 0x63) || (name_len == 13 && letter == 0x61)
            v0 = colon + 1
            v0 += 1 while v0 < eol && ((b = buffer.getbyte(v0)) == 32 || b == 9)
            v1 = eol
            v1 -= 1 while v1 > v0 && ((b = buffer.getbyte(v1 - 1)) == 32 || b == 9)
            value = buffer.byteslice(v0, v1 - v0)
            name = buffer.byteslice(pos, name_len)
            case name_len
            when 14 # content-length
              if name.casecmp?(CONTENT_LENGTH)
                return reject(socket, 400) unless DIGITS.match?(value)

                length = value.to_i
              end
            when 17 # transfer-encoding: request bodies must use Content-Length
              return reject(socket, 411) if name.casecmp?(TRANSFER_ENCODING)
            when 10 # connection
              if name.casecmp?(CONNECTION)
                if CLOSE_TOKEN.match?(value) then close = true
                elsif http10 && KEEP_ALIVE_TOKEN.match?(value) then close = false
                end
              end
            else # authorization
              authorization = value if name.casecmp?(AUTHORIZATION)
            end
          end
        end
        pos = eol + 2
      end
      return reject(socket, 413) if length > MAX_BODY

      body_start = head_end + 4
      break if size < body_start + length # the body hasn't fully arrived yet

      body = buffer.byteslice(body_start, length) if length.positive?
      status, document = @app.call(method, path, authorization, body)
      close ||= shed || @open > @budget
      respond(socket, close ? CLOSE : (http10 ? KEEP_10 : KEEP), status, document)
      return nil if close

      position = body_start + length
    end

    if position.zero? then buffer
    elsif position >= size then EMPTY
    else buffer.byteslice(position, size - position)
    end
  end

  def reject(socket, status)
    respond(socket, CLOSE, status, %({"error":"#{REASONS[status].downcase}"}))
    nil
  end

  # One interpolation: `String.new` with a keyword argument cost two extra allocations per response.
  def respond(socket, heads, status, document)
    write(socket, "#{heads[status] || heads[500]}#{document.bytesize}\r\n\r\n#{document}",
          heads.equal?(CLOSE))
  end

  # Through the fiber scheduler: with io_uring, every worker's write is queued and submitted together.
  # A connection's last response is sent with MSG_MORE and `shutdown` raises the FIN with the held
  # segment; `close` alone would reset the connection and discard it while unread input is queued.
  def write(socket, data, last = false)
    return socket.write(data) unless last

    size = data.bytesize
    sent = socket.send(data, Socket::MSG_MORE)
    sent += socket.send(data.byteslice(sent, size - sent), Socket::MSG_MORE) while sent < size
    socket.shutdown(Socket::SHUT_WR)
  end
end
