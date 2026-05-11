"""Allow `python3 -m shop_msg ...` to invoke the CLI."""
import sys

from shop_msg.cli import main

if __name__ == "__main__":
    sys.exit(main())
