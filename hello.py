"""Print a greeting a given number of times."""

import argparse


def say_hello(n: int, name: str = "world") -> None:
    """Print "Hello, <name>!" n times."""
    for _ in range(n):
        print(f"Hello, {name}!")


def main() -> None:
    parser = argparse.ArgumentParser(description="Say hello n times.")
    parser.add_argument("n", type=int, nargs="?", default=1, help="how many times to greet")
    parser.add_argument("--name", default="world", help="who to greet")
    args = parser.parse_args()

    if args.n < 0:
        parser.error("n must be zero or positive")

    say_hello(args.n, args.name)


if __name__ == "__main__":
    main()
