# Ansible + PostgreSQL

Второе дз по DevOps-2. Нужно было написать свою ансибл-роль, которая ставит и настраивает постгрю на вм, создаёт бд и юзеров, а также поднимает postgres-exporter.

Всё делал на локальной вм с Ubuntu Server 24.04. Ансибл запускал с основной машины на Fedora, подключение к вм через SSH.

Готовую роль для постгри из Galaxy не использовал, задачи писал сам. Из Galaxy ставится только коллекция community.postgresql, чтобы работать с бд через модули ансибла.

## Что получилось

1. Постгря ставится из стандартных репо убунты, без дополнительных источников.
2. Данные хранятся в /data/postgres/data. Кластер инициализируется через pg_ctl initdb.
3. Настройки postgresql.conf и pg_hba.conf задаются через переменные роли.
4. PostgreSQL запускается через свою службу systemd с автозапуском.
5. Создаются бд app_db и юзер app_user, пароль хранится в Ansible Vault.
6. Дополнительно ставится postgres-exporter с отдельным юзером для мониторинга.
7. Повторный запуск роли не должен ничего менять, если все настройки уже применены.

Для изменений конфигов и службы экспортера используются хендлеры. Т.е. лишний раз рестартовать сервисы при каждом прогоне не нужно.

## Структура репо

```text
.
├── inventories/
│   └── main/
│       ├── group_vars/
│       │   └── all/
│       │       └── vault.yml
│       └── hosts.yaml
├── roles/
│   └── postgres/
│       ├── defaults/
│       ├── handlers/
│       ├── tasks/
│       ├── templates/
│       └── README.md
├── ansible.cfg
├── postgres.play.yml
├── requirements.yml
└── README.md
```

Основная логика находится в roles/postgres/tasks/main.yml, переменные в defaults/main.yml, а шаблоны конфигов и служб в templates.

В ansible.cfg указаны пути к инвентарю и ролям, поэтому при запуске их отдельно прописывать не нужно.

Подробное описание переменных и самой роли оставил в roles/postgres/README.md.

## Как запустить

Нужна машина с установленным Ansible и вм на Ubuntu 24.04, доступная по SSH. В инвентаре vm1 нужно указать актуальный IP, юзера и путь к SSH-ключу. У юзера должны быть права sudo, тк роль ставит пакеты и настраивает systemd.

Сначала ставлю нужную коллекцию:

```bash
ansible-galaxy collection install -r requirements.yml
```

Потом из корня репо запускаю плейбук:

```bash
ansible-playbook postgres.play.yml --ask-vault-pass
```

Пароль волта нужен для расшифровки пароля юзера бд. Сам зашифрованный файл лежит в inventories/main/group_vars/all/vault.yml.

## Проверка

После установки проверяю, что постгря и экспортер запущены:

```bash
ansible vm1 -b --ask-vault-pass -m command -a 'systemctl is-active postgresql-custom postgres-exporter'
```

Обе службы должны быть active.

Дальше проверяю подключение к бд:

```bash
ansible vm1 -b --ask-vault-pass --become-user postgres -m command -a 'psql -c "select 1"'
```

Можно также посмотреть созданные бд и юзеров:

```bash
ansible vm1 -b --ask-vault-pass --become-user postgres -m command -a 'psql -c "\l"'
ansible vm1 -b --ask-vault-pass --become-user postgres -m command -a 'psql -c "\du"'
```

Экспортер слушает локальный порт 9187 на вм. Проверяю его через ансибл:

```bash
ansible vm1 --ask-vault-pass -m uri -a 'url=http://127.0.0.1:9187/metrics return_content=true' | grep -o 'pg_up 1'
```

Если получаю pg_up 1, значит экспортер подключился к постгре и метрики собираются.

## Идемпотентность

Один из основных пунктов дз, поэтому отдельно запускал плейбук повторно:

```bash
ansible-playbook postgres.play.yml --ask-vault-pass
```

На повторном прогоне получил changed=0 и failed=0. То есть, если ничего не менялось, ансибл просто проверяет состояние и не выполняет лишних изменений.

Полные логи прогонов и команд лежат в двух текстовых файлах.
