# Variants

Each `<name>.patch` is one build directory of the campaign, stored as a git diff against `submissions/ruby-async-cicloid`. Rebuild the directories from the repository root:

```bash
for p in local-bench/variants/*.patch; do d="${p%.patch}"; [ -e "$d" ] || { cp -Rp submissions/ruby-async-cicloid "$d" && git apply --directory="$d" "$p"; }; done
```

Existing directories are left alone; delete one to rebuild it. The rebuilt directories are gitignored, and `local-bench/variants/<name>` in `RESULTS.md` means the rebuilt directory. `final2` is the tree submitted as `submissions/ruby-async-simpl1g`, except that the submission's README.md replaces the local `## stack7` / `## final2` notes with a `## Changes against ruby-async-cicloid` section.
