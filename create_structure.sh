#!/bin/bash

mkdir -p project_root/{database/migrations,handlers,keyboards,utils,logs}
touch project_root/.env
touch project_root/requirements.txt
touch project_root/README.md
touch project_root/bot.py
touch project_root/config.py

touch project_root/database/__init__.py
touch project_root/database/models.py
touch project_root/database/db.py

touch project_root/handlers/__init__.py
touch project_root/handlers/common.py
touch project_root/handlers/user.py
touch project_root/handlers/admin.py
touch project_root/handlers/manager.py

touch project_root/keyboards/__init__.py
touch project_root/keyboards/main_menu.py
touch project_root/keyboards/inline.py
touch project_root/keyboards/callbacks.py

touch project_root/utils/__init__.py
touch project_root/utils/google_sheets.py
touch project_root/utils/notifications.py
touch project_root/utils/validators.py
