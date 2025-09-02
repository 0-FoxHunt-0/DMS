import sys
from src.cli import app

if __name__ == "__main__":
    # Backward/alternate CLI compatibility: allow optional 'send' subcommand token
    # so both `python main.py send ...` and `python main.py ...` work.
    if len(sys.argv) > 1 and sys.argv[1].lower() == "send":
        del sys.argv[1]
    app()

