#!/bin/bash
# Benchmark: Python vs C Modbus Performance Comparison

set -e

PYTHON_SCRIPT="../ebyteserrequest.py"
C_BINARY="./test_modbus_relay"
ITERATIONS=10

echo "========================================================"
echo "MODBUS RELAY PERFORMANCE BENCHMARK"
echo "Python vs C Implementation"
echo "========================================================"
echo ""

# Check if binaries exist
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "Error: Python script not found: $PYTHON_SCRIPT"
    exit 1
fi

if [ ! -f "$C_BINARY" ]; then
    echo "Error: C binary not found. Run 'make test-modbus' first."
    exit 1
fi

# Test states
TEST_STATES=(0 7 5 3 0)

echo "Test Configuration:"
echo "  Iterations per test: $ITERATIONS"
echo "  Test states: ${TEST_STATES[*]}"
echo "  Port: /dev/ttyAMA0 @ 9600 baud"
echo ""

# Python benchmark
echo "=== PYTHON BENCHMARK ==="
echo ""

PYTHON_TOTAL=0

for state in "${TEST_STATES[@]}"; do
    echo -n "State $state: "

    TIMES=()
    for i in $(seq 1 $ITERATIONS); do
        START=$(date +%s%N)
        python3 "$PYTHON_SCRIPT" "$state" > /dev/null 2>&1
        END=$(date +%s%N)

        ELAPSED=$(( (END - START) / 1000000 ))  # Convert to ms
        TIMES+=($ELAPSED)
        PYTHON_TOTAL=$((PYTHON_TOTAL + ELAPSED))

        sleep 0.1
    done

    # Calculate average
    AVG=0
    for t in "${TIMES[@]}"; do
        AVG=$((AVG + t))
    done
    AVG=$((AVG / ITERATIONS))

    # Calculate min/max
    MIN=${TIMES[0]}
    MAX=${TIMES[0]}
    for t in "${TIMES[@]}"; do
        [ $t -lt $MIN ] && MIN=$t
        [ $t -gt $MAX ] && MAX=$t
    done

    echo "avg=${AVG}ms, min=${MIN}ms, max=${MAX}ms"
done

PYTHON_AVG=$((PYTHON_TOTAL / (${#TEST_STATES[@]} * ITERATIONS)))

echo ""
echo "Python total: $PYTHON_TOTAL ms"
echo "Python average: $PYTHON_AVG ms per write"
echo ""

# C benchmark
echo "=== C BENCHMARK ==="
echo ""

C_TOTAL=0

for state in "${TEST_STATES[@]}"; do
    echo -n "State $state: "

    TIMES=()
    for i in $(seq 1 $ITERATIONS); do
        # Parse output for timing
        OUTPUT=$("$C_BINARY" "$state" 2>&1)

        # Extract microseconds from output like "Operation took 1234 µs"
        ELAPSED=$(echo "$OUTPUT" | grep -oP 'Operation took \K\d+' || echo "0")

        if [ "$ELAPSED" = "0" ]; then
            echo "Error parsing C output"
            exit 1
        fi

        ELAPSED_MS=$((ELAPSED / 1000))  # Convert to ms
        TIMES+=($ELAPSED_MS)
        C_TOTAL=$((C_TOTAL + ELAPSED_MS))

        sleep 0.1
    done

    # Calculate average
    AVG=0
    for t in "${TIMES[@]}"; do
        AVG=$((AVG + t))
    done
    AVG=$((AVG / ITERATIONS))

    # Calculate min/max
    MIN=${TIMES[0]}
    MAX=${TIMES[0]}
    for t in "${TIMES[@]}"; do
        [ $t -lt $MIN ] && MIN=$t
        [ $t -gt $MAX ] && MAX=$t
    done

    echo "avg=${AVG}ms, min=${MIN}ms, max=${MAX}ms"
done

C_AVG=$((C_TOTAL / (${#TEST_STATES[@]} * ITERATIONS)))

echo ""
echo "C total: $C_TOTAL ms"
echo "C average: $C_AVG ms per write"
echo ""

# Comparison
echo "========================================================"
echo "COMPARISON"
echo "========================================================"
echo ""
printf "%-20s %10s %10s\n" "Implementation" "Avg Time" "Total Time"
printf "%-20s %10s %10s\n" "----------------" "--------" "----------"
printf "%-20s %8d ms %8d ms\n" "Python" $PYTHON_AVG $PYTHON_TOTAL
printf "%-20s %8d ms %8d ms\n" "C" $C_AVG $C_TOTAL
echo ""

# Calculate speedup
if [ $C_AVG -gt 0 ]; then
    SPEEDUP=$((PYTHON_AVG * 100 / C_AVG))
    echo "Speedup: C is ${SPEEDUP}% of Python speed ($(echo "scale=1; $PYTHON_AVG / $C_AVG" | bc)x faster)"
else
    echo "C is significantly faster"
fi

echo ""
echo "========================================================"
echo "MULTIPLEXING ANALYSIS"
echo "========================================================"
echo ""

echo "For 2-second slot with Soyo inverter:"
echo ""
printf "  %-30s %8d ms (%.1f%%)\n" "Python Modbus write:" $PYTHON_AVG $(echo "scale=1; $PYTHON_AVG / 20" | bc)
printf "  %-30s %8d ms (%.1f%%)\n" "C Modbus write:" $C_AVG $(echo "scale=1; $C_AVG / 20" | bc)
printf "  %-30s %8d ms (%.1f%%)\n" "Baudrate switch (2x):" 10 0.5
echo ""

PYTHON_OVERHEAD=$((PYTHON_AVG + 10))
C_OVERHEAD=$((C_AVG + 10))

printf "Total overhead with Python: %d ms (%.1f%% of 2s slot)\n" \
    $PYTHON_OVERHEAD $(echo "scale=1; $PYTHON_OVERHEAD / 20" | bc)
printf "Total overhead with C:      %d ms (%.1f%% of 2s slot)\n" \
    $C_OVERHEAD $(echo "scale=1; $C_OVERHEAD / 20" | bc)

echo ""
echo "Conclusion:"
if [ $PYTHON_OVERHEAD -lt 200 ]; then
    echo "  ✓ Python: FEASIBLE (< 10% overhead)"
else
    echo "  ✗ Python: HIGH overhead"
fi

if [ $C_OVERHEAD -lt 200 ]; then
    echo "  ✓ C: FEASIBLE (< 10% overhead)"
else
    echo "  ✗ C: HIGH overhead"
fi

echo ""
echo "========================================================"
