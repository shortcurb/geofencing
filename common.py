import os

def validate_env_vars(env_var_list) -> None:
    """
    Validate that all required environment variables are set.
    """
    for var in env_var_list:
        if not os.getenv(var):
            raise EnvironmentError(f"Environment variable {var} is not set")
