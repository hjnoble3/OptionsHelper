import os
from datetime import datetime, timezone

from dotenv import load_dotenv, set_key
from tastytrade import Session
from tastytrade.utils import TastytradeError

DOTENV_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))

active_session = None


def clear_remember_token_env():
    """Clears the TASTYWORKS_REMEMBER_TOKEN from the .env file and current os.environ."""
    print("Clearing remember token...")
    if os.path.exists(DOTENV_PATH):
        # Set the token to an empty string in the .env file
        set_key(DOTENV_PATH, "TASTYWORKS_REMEMBER_TOKEN", "")
        print(f"Cleared TASTYWORKS_REMEMBER_TOKEN in {DOTENV_PATH}")
    else:
        print(f"{DOTENV_PATH} not found, cannot clear token from file.")
    # Remove from current environment variables
    os.environ.pop("TASTYWORKS_REMEMBER_TOKEN", None)


def session_login():
    """Login to TastyTrade.
    Manages a module-level active session. If a valid session exists, it's reused.
    Loads environment variables from a .env file.
    Prioritizes remember_token for login. If not available or expired,
    uses username and password, and saves a new remember_token.

    Returns a session object.
    """
    global active_session  # Ensure we are working with the module-level active_session

    # Check if there's an existing, valid session
    if active_session:
        try:
            is_valid_and_not_expired = False
            if hasattr(active_session, "session_expiration") and active_session.session_expiration > datetime.now(timezone.utc):
                if active_session.validate():  # validate() can make an API call
                    is_valid_and_not_expired = True
                else:
                    print("Existing session failed API validation.")
            else:
                print("Existing session is expired or lacks expiration info.")

            if is_valid_and_not_expired:
                print("Using existing active session.")
                return active_session
            print("Existing session is invalid/expired. Will attempt new login.")
            active_session = None  # Mark for re-login
        except Exception as e:
            print(f"Error validating existing session: {e}. Will attempt new login.")
            active_session = None  # Mark for re-login

    # If active_session is None at this point, we need to log in.
    if not active_session:
        print("Attempting to establish a new session...")

    # Load environment variables from .env file.
    # override=True ensures that if .env is changed and this function is called again,
    # os.environ reflects the latest values from the file.
    load_dotenv(dotenv_path=DOTENV_PATH, override=True)
    username = os.environ.get("TASTYWORKS_LOGIN")
    password = os.environ.get("TASTYWORKS_PASSWORD")
    remember_token_env = os.environ.get("TASTYWORKS_REMEMBER_TOKEN")

    # Try to log in with remember_token if available
    if not active_session and remember_token_env:
        print("Attempting login with remember token...")
        try:
            # The Session class requires 'login' (username) even when using remember_token
            temp_session = Session(login=username, remember_token=remember_token_env, is_test=False)
            if temp_session.validate():
                print("Login with remember token successful and session is active.")
                active_session = temp_session
            else:
                print("Remember token did not result in a valid session (it might be invalid or expired).")
                clear_remember_token_env()
        except TastytradeError as e:
            print(f"Error logging in with remember token: {e}.")
            clear_remember_token_env()
            # Do not raise, allow fallback to password login
        except Exception as e:  # Catch any other unexpected errors during token login
            print(f"Unexpected error during remember token login: {e}.")
            clear_remember_token_env()
            # Do not raise, allow fallback to password login

    # If token login failed or no token was available, try password login
    if not active_session:
        print("Attempting login with username and password...")
        if not password:
            raise ValueError(
                "TASTYWORKS_PASSWORD must be set in .env file or environment variables for password login.",
            )

        try:
            # Use remember_me=True to request a new remember_token
            active_session = Session(login=username, password=password, remember_me=True, is_test=False)
            print("Login with username and password successful.")

            if active_session.remember_token:
                print("New remember token obtained. Saving to .env file.")
                if os.path.exists(DOTENV_PATH):
                    set_key(DOTENV_PATH, "TASTYWORKS_REMEMBER_TOKEN", active_session.remember_token)
                    # Update os.environ as well, so the current process uses the new token if session_login is called again
                    os.environ["TASTYWORKS_REMEMBER_TOKEN"] = active_session.remember_token
                    print(f"New remember token saved to {DOTENV_PATH} and updated in current environment.")
                else:
                    # This might happen if DOTENV_PATH is misconfigured or .env is not where expected.
                    print(
                        f"{DOTENV_PATH} not found by os.path.exists. "
                        "New remember token not saved to .env file automatically. "
                        "It is available in the current environment for this session.",
                    )
            else:
                # This is unexpected if remember_me=True and successful login
                print("Warning: No new remember token received despite remember_me=True and successful login.")
        except TastytradeError as e:
            print(f"Error logging in with username/password: {e}")
            raise  # Re-raise the error to signal login failure to the caller
        except Exception as e:  # Catch any other unexpected errors
            print(f"Unexpected error during username/password login: {e}")
            raise

    if not active_session:
        # This line should ideally not be reached if errors above are re-raised.
        # However, as a final safeguard:
        raise TastytradeError("Failed to establish a session after all attempts.")
    return active_session


def destroy_session(session):
    """Destroys the given session."""
    if session:
        print("Destroying session...")
        session.destroy()
    else:
        print("No session to destroy.")


### Uncomment to test
# if __name__ == "__main__":
#     try:
#         session = session_login()
#         print("Login successful!")
#         # You can add more testing code here, e.g., accessing account information
#         # destroy_session(session)
#     except Exception as e:
#         print(f"Login failed: {e}")
