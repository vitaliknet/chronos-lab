import logging
from pathlib import Path
import shutil
import sys

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(name)s | %(funcName)s | %(levelname)s:%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _init_config():
    try:
        config_dir = Path.home() / ".chronos_lab"
        env_file = config_dir / ".env"

        if not env_file.exists():
            config_dir.mkdir(parents=True, exist_ok=True)

            package_dir = Path(__file__).parent
            env_example = package_dir / ".env.example"

            if env_example.exists():
                shutil.copy(env_example, env_file)
                print(f"✓ Chronos Lab: Created config at {env_file}", file=sys.stderr)

    except Exception as e:
        print(f"Warning: Could not initialize Chronos Lab config: {e}", file=sys.stderr)


_init_config()

del _init_config
