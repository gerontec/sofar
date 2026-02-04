#!/usr/bin/env python3
"""
Pivot Register Data - Converts raw CSV register data to pivoted format

Reads raw register data (section, name, value, unit) and pivots it
so that register names become columns and sections become rows.
"""

import sys
import argparse
import csv
import pandas as pd
from pathlib import Path

VERSION = "v1.0"

def pivot_registers(input_csv: str, output_csv: str, verbose: bool = False) -> bool:
    """
    Pivot register data from raw format to pivoted format.

    Args:
        input_csv: Path to input raw CSV file
        output_csv: Path to output pivoted CSV file
        verbose: Print detailed information

    Returns:
        True if successful, False otherwise
    """
    try:
        # Read raw CSV
        if verbose:
            print(f"Reading raw data from: {input_csv}")

        df = pd.read_csv(input_csv)

        if df.empty:
            print(f"Warning: Input file is empty: {input_csv}", file=sys.stderr)
            return False

        if verbose:
            print(f"Read {len(df)} register entries")
            print(f"Columns: {', '.join(df.columns)}")

        # Verify required columns
        required_cols = ['section', 'name', 'value']
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            print(f"Error: Missing required columns: {', '.join(missing)}", file=sys.stderr)
            print(f"Available columns: {', '.join(df.columns)}", file=sys.stderr)
            return False

        # Remove the "I General" prefix from section names (if present)
        df['section'] = df['section'].str.replace(r'^I General\s*[（(].*?[）)]?\s*', '', regex=True)

        # Pivot: sections as rows, register names as columns
        if verbose:
            print("Pivoting data...")

        pivoted = df.pivot(index='section', columns='name', values='value')
        pivoted = pivoted.reset_index()

        # Ensure 'section' is the first column
        cols = ['section'] + [col for col in pivoted.columns if col != 'section']
        pivoted = pivoted[cols]

        # Save pivoted data
        if verbose:
            print(f"Writing pivoted data to: {output_csv}")
            print(f"Output shape: {pivoted.shape[0]} rows × {pivoted.shape[1]} columns")

        pivoted.to_csv(output_csv, index=False, quoting=csv.QUOTE_NONNUMERIC)

        if verbose or True:  # Always print success message
            print(f"Successfully pivoted {len(df)} registers into {pivoted.shape[1]-1} columns")
            print(f"Output saved to: {output_csv}")

        return True

    except FileNotFoundError:
        print(f"Error: Input file not found: {input_csv}", file=sys.stderr)
        return False
    except pd.errors.EmptyDataError:
        print(f"Error: Input file is empty: {input_csv}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Error pivoting data: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(
        description='Pivot register data from raw CSV to pivoted format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s                                    # Use defaults
  %(prog)s -i /tmp/raw.csv -o /tmp/pivoted.csv
  %(prog)s --input data.csv --output result.csv -v
  %(prog)s --version

Default paths:
  Input:  /tmp/raw.csv
  Output: /tmp/pivoted_registers.csv
        '''
    )

    parser.add_argument('-i', '--input',
                        default='/tmp/raw.csv',
                        help='Input raw CSV file (default: /tmp/raw.csv)')

    parser.add_argument('-o', '--output',
                        default='/tmp/pivoted_registers.csv',
                        help='Output pivoted CSV file (default: /tmp/pivoted_registers.csv)')

    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help='Print detailed information')

    parser.add_argument('--version',
                        action='version',
                        version=f'%(prog)s {VERSION}')

    args = parser.parse_args()

    # Verify input file exists
    if not Path(args.input).exists():
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        print(f"Run 'read_inverter' first to generate the raw CSV file.", file=sys.stderr)
        sys.exit(1)

    # Perform pivot
    success = pivot_registers(args.input, args.output, args.verbose)

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
