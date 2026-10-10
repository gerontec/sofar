#!/bin/sh
# tmp-log-budget: keeps all /tmp/*.log* together below BUDGET bytes (user 2026-10-10: logs < 20 MB; /tmp is a
# 100 MB tmpfs that MariaDB also needs for temp tables). Old rotations (*.log.N, *.gz) go first, then the largest live
# log is cut to its last quarter in place (same inode: the writing scripts keep appending). Hourly from
# /etc/cron.d/tmp-log-budget.
BUDGET=$((15 * 1024 * 1024))
i=0
while [ $i -lt 50 ]; do
	total=$(find /tmp -maxdepth 1 -type f -name '*.log*' -printf '%s\n' | awk '{s += $1} END {print s + 0}')
	[ "$total" -le "$BUDGET" ] && exit 0
	old=$(find /tmp -maxdepth 1 -type f \( -name '*.log.[0-9]*' -o -name '*.log*.gz' \) -printf '%s %p\n' \
		| sort -rn | head -n 1 | cut -d' ' -f2-)
	if [ -n "$old" ]; then
		logger -t tmp-log-budget "total $total > $BUDGET: removed $old"
		rm -f "$old"
	else
		big=$(find /tmp -maxdepth 1 -type f -name '*.log' -printf '%s %p\n' | sort -rn | head -n 1)
		size=${big%% *}
		f=${big#* }
		[ -n "$f" ] || exit 0
		logger -t tmp-log-budget "total $total > $BUDGET: cut $f ($size bytes) to its last quarter"
		tail -c $((size / 4)) "$f" > "$f.cut" && cat "$f.cut" > "$f"
		rm -f "$f.cut"
	fi
	i=$((i + 1))
done
