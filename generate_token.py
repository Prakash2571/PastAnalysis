#!/usr/bin/env python3
"""
Kite Connect Access Token Generator
====================================

Generates the daily access_token needed by the backfill script.
Kite tokens expire every day at 6 AM IST, so you need to run this once per day.

Two modes:
  1. MANUAL (default): Opens a URL, you login, paste the redirect URL back.
  2. AUTO (with request_token): If you already have a request_token, pass it directly.

Usage:
  python generate_token.py                          # interactive login
  python generate_token.py --request-token abc123   # if you have the token already

After running, it updates .env with the new KITE_ACCESS_TOKEN.
"""

import argparse
import os
import sys
import webbrowser

from dotenv import load_dotenv, set_key
from kiteconnect import KiteConnect

ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")


def main():
    load_dotenv(ENV_FILE)

    parser = argparse.ArgumentParser(description="Generate Kite access token")
    parser.add_argument("--request-token", help="Request token from redirect URL")
    args = parser.parse_args()

    api_key = os.getenv("KITE_API_KEY")
    api_secret = os.getenv("KITE_API_SECRET")

    if not api_key or not api_secret:
        sys.exit(
            "ERROR: Set KITE_API_KEY and KITE_API_SECRET in .env first.\n"
            "Get them from https://developers.kite.trade/apps"
        )

    kite = KiteConnect(api_key=api_key)

    if args.request_token:
        request_token = args.request_token
    else:
        # Generate login URL and ask user to authenticate
        login_url = kite.login_url()
        print(f"\n1. Open this URL in your browser and login:\n")
        print(f"   {login_url}\n")
        print(f"2. After login, you'll be redirected to your redirect URL.")
        print(f"   The URL will look like: https://yourapp.com/?request_token=XXXXX&action=login")
        print(f"   Copy the 'request_token' value from that URL.\n")

        try:
            webbrowser.open(login_url)
        except Exception:
            pass  # might not have a browser (e.g., on server)

        request_token = input("3. Paste the request_token here: ").strip()

    if not request_token:
        sys.exit("ERROR: No request_token provided.")

    # Generate access token
    try:
        data = kite.generate_session(request_token, api_secret=api_secret)
        access_token = data["access_token"]
    except Exception as e:
        sys.exit(f"ERROR generating session: {e}")

    # Save to .env
    set_key(ENV_FILE, "KITE_ACCESS_TOKEN", access_token)

    print(f"\nAccess token generated and saved to .env!")
    print(f"Token: {access_token[:10]}...{access_token[-5:]}")
    print(f"\nValid until ~6:00 AM IST tomorrow.")
    print(f"Now run:  python hourly_futures_backfill.py")


if __name__ == "__main__":
    main()
