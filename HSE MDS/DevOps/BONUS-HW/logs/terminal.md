# BONUS-HW — журнал выполнения url-probe

Бонусное задание по DevOps-2. Здесь собраны реальные команды и выводы из терминала Fedora, в том порядке, в котором я выполнял работу.

Проверял Docker-образ, запуск через cron, обновление URL без рестарта, перезапуск контейнера, передачу метрик через Node Exporter и срабатывание алерта в Prometheus.

Запись взята из терминального лога от 25.10.2026. Команды и результаты проверок сохранены, включая ошибки и повторные попытки.

## 1. Проверка исходного скрипта

Сначала запустил готовый probe.py на Fedora. В списке сразу были четыре адреса: рабочий сайт, 404, несуществующий домен и адрес с таймаутом.

```text
$ cd ~/projects/study-notes/"HSE MDS"/DevOps/BONUS-HW
```

```text
$ script -q -f logs/terminal.log
```

```text
$ python3 url-probe/probe.py url-probe/urls.txt
OK   200  0.171s https://example.com
FAIL 404  0.099s https://example.com/no-such-page
http://no-such-host.invalid: <urlopen error [Errno -2] Name or service not known>
FAIL   0  0.001s http://no-such-host.invalid
http://10.255.255.1: <urlopen error timed out>
FAIL   0  5.006s http://10.255.255.1
# HELP probe_success 1, если получен ответ с кодом 2xx/3xx, иначе 0
# TYPE probe_success gauge
probe_success{url="https://example.com"} 1
probe_success{url="https://example.com/no-such-page"} 0
probe_success{url="http://no-such-host.invalid"} 0
probe_success{url="http://10.255.255.1"} 0
# HELP probe_http_status_code Код ответа HTTP, 0 - ответа нет
# TYPE probe_http_status_code gauge
probe_http_status_code{url="https://example.com"} 200
probe_http_status_code{url="https://example.com/no-such-page"} 404
probe_http_status_code{url="http://no-such-host.invalid"} 0
probe_http_status_code{url="http://10.255.255.1"} 0
# HELP probe_duration_seconds Сколько заняла проверка
# TYPE probe_duration_seconds gauge
probe_duration_seconds{url="https://example.com"} 0.171
probe_duration_seconds{url="https://example.com/no-such-page"} 0.099
probe_duration_seconds{url="http://no-such-host.invalid"} 0.001
probe_duration_seconds{url="http://10.255.255.1"} 5.006
# HELP probe_last_run_timestamp_seconds Время последнего запуска
# TYPE probe_last_run_timestamp_seconds gauge
```

## 2. Docker-образ и запуск без root

Собрал небольшой образ на Python Alpine. Проверил его размер и UID пользователя. При первом подключении каталога получил отказ в доступе из-за SELinux на Fedora, поэтому повторил запуск с меткой `:Z`. Отдельно проверил переменную PROBE_TIMEOUT.

```text
$ nano url-probe/Dockerfile
```

```text
$ cat url-probe/Dockerfile
FROM python:3.12-alpine

RUN adduser -D probe

WORKDIR /app

COPY probe.py .

USER probe

CMD ["python3", "probe.py", "/config/urls.txt"]
```

```text
$ docker build -t url-probe:hw ./url-probe
[+] Building 10.9s (9/9) FINISHED                                docker:default
 => [internal] load build definition from Dockerfile                       0.0s
 => => transferring dockerfile: 236B                                       0.0s
 => [internal] load metadata for docker.io/library/python:3.12-alpine      2.2s
 => [internal] load .dockerignore                                          0.0s
 => => transferring context: 2B                                            0.0s
 => [1/4] FROM docker.io/library/python:3.12-alpine@sha256:b658b05ce9c4eb  7.7s
 => => resolve docker.io/library/python:3.12-alpine@sha256:b658b05ce9c4eb  0.0s
 => => sha256:39a526054cd95b7ad4bb7cce9c5c6d27dcc22e8e82e6b71 247B / 247B  0.2s
 => => sha256:37737182bc17ec146b765dac3e7b5796ff774014f 13.78MB / 13.78MB  7.3s
 => => sha256:6505b46c81c2f2e17de7ef7fcfe645c68e055dc 412.02kB / 412.02kB  1.5s
 => => extracting sha256:6505b46c81c2f2e17de7ef7fcfe645c68e055dc0a7573a3a  0.1s
 => => extracting sha256:37737182bc17ec146b765dac3e7b5796ff774014fd61ac96  0.3s
 => => extracting sha256:39a526054cd95b7ad4bb7cce9c5c6d27dcc22e8e82e6b71d  0.0s
 => [internal] load build context                                          0.0s
 => => transferring context: 4.01kB                                        0.0s
 => [2/4] RUN adduser -D probe                                             0.5s
 => [3/4] WORKDIR /app                                                     0.0s
 => [4/4] COPY probe.py .                                                  0.0s
 => exporting to image                                                     0.2s
 => => exporting layers                                                    0.1s
 => => exporting manifest sha256:3d111ca5c7c2694fe27961857526eb08b5510b29  0.0s
 => => exporting config sha256:f3059ef2e464366b406eb3f1bba67261974f7da09b  0.0s
 => => exporting attestation manifest sha256:dbfec88689e0cb734e4a4e4f637b  0.0s
 => => exporting manifest list sha256:484a182113334829d1b0f05b1dcf610d0c1  0.0s
 => => naming to docker.io/library/url-probe:hw                            0.0s
 => => unpacking to docker.io/library/url-probe:hw                         0.0s
```

```text
$ docker images url-probe:hw
                                                            i Info →   U  In Use
IMAGE          ID             DISK USAGE   CONTENT SIZE   EXTRA
url-probe:hw   484a18211333       74.7MB         18.1MB
```

```text
$ docker run --rm url-probe:hw id
uid=1000(probe) gid=1000(probe) groups=1000(probe)
```

```text
$ docker run --rm -v "$PWD/url-probe:/config:ro" url-probe:hw
cannot read urls file: [Errno 13] Permission denied: '/config/urls.txt'
```

```text
$ ls -ldZ url-probe url-probe/urls.txt
drwxr-xr-x   - tadzhnahal unconfined_u:object_r:user_home_t:s0 25 окт 15:41 url-probe
.rw-r--r-- 208 tadzhnahal unconfined_u:object_r:user_home_t:s0 25 окт 15:36 url-probe/urls.txt
```

```text
$ docker run --rm -v "$PWD/url-probe:/config:ro,Z" url-probe:hw
OK   200  0.135s https://example.com
FAIL 404  0.123s https://example.com/no-such-page
http://no-such-host.invalid: <urlopen error [Errno -2] Name does not resolve>
FAIL   0  0.055s http://no-such-host.invalid
http://10.255.255.1: <urlopen error timed out>
FAIL   0  5.006s http://10.255.255.1
# HELP probe_success 1, если получен ответ с кодом 2xx/3xx, иначе 0
# TYPE probe_success gauge
probe_success{url="https://example.com"} 1
probe_success{url="https://example.com/no-such-page"} 0
probe_success{url="http://no-such-host.invalid"} 0
probe_success{url="http://10.255.255.1"} 0
# HELP probe_http_status_code Код ответа HTTP, 0 - ответа нет
# TYPE probe_http_status_code gauge
probe_http_status_code{url="https://example.com"} 200
probe_http_status_code{url="https://example.com/no-such-page"} 404
probe_http_status_code{url="http://no-such-host.invalid"} 0
probe_http_status_code{url="http://10.255.255.1"} 0
# HELP probe_duration_seconds Сколько заняла проверка
# TYPE probe_duration_seconds gauge
probe_duration_seconds{url="https://example.com"} 0.135
probe_duration_seconds{url="https://example.com/no-such-page"} 0.123
probe_duration_seconds{url="http://no-such-host.invalid"} 0.055
probe_duration_seconds{url="http://10.255.255.1"} 5.006
# HELP probe_last_run_timestamp_seconds Время последнего запуска
# TYPE probe_last_run_timestamp_seconds gauge
probe_last_run_timestamp_seconds 1792932325
```

```text
$ docker run --rm -e PROBE_TIMEOUT=2 -v "$PWD/url-probe:/config:ro,Z" url-probe:hw
OK   200  0.131s https://example.com
FAIL 404  0.101s https://example.com/no-such-page
http://no-such-host.invalid: <urlopen error [Errno -2] Name does not resolve>
FAIL   0  0.039s http://no-such-host.invalid
http://10.255.255.1: <urlopen error timed out>
FAIL   0  2.003s http://10.255.255.1
# HELP probe_success 1, если получен ответ с кодом 2xx/3xx, иначе 0
# TYPE probe_success gauge
probe_success{url="https://example.com"} 1
probe_success{url="https://example.com/no-such-page"} 0
probe_success{url="http://no-such-host.invalid"} 0
probe_success{url="http://10.255.255.1"} 0
# HELP probe_http_status_code Код ответа HTTP, 0 - ответа нет
# TYPE probe_http_status_code gauge
probe_http_status_code{url="https://example.com"} 200
probe_http_status_code{url="https://example.com/no-such-page"} 404
probe_http_status_code{url="http://no-such-host.invalid"} 0
probe_http_status_code{url="http://10.255.255.1"} 0
# HELP probe_duration_seconds Сколько заняла проверка
# TYPE probe_duration_seconds gauge
probe_duration_seconds{url="https://example.com"} 0.131
probe_duration_seconds{url="https://example.com/no-such-page"} 0.101
probe_duration_seconds{url="http://no-such-host.invalid"} 0.039
probe_duration_seconds{url="http://10.255.255.1"} 2.003
# HELP probe_last_run_timestamp_seconds Время последнего запуска
# TYPE probe_last_run_timestamp_seconds gauge
probe_last_run_timestamp_seconds 1792932376
```

## 3. Запуск по расписанию через cron

Добавил crontab и простой скрипт запуска, затем пересобрал образ. Cron выполняет проверку раз в минуту от пользователя probe, а строки OK/FAIL и метрики попадают в docker logs.

```text
$ nano url-probe/crontab
```

```text
$ cat url-probe/crontab
* * * * * /app/run-probe.sh
```

```text
$ nano url-probe/run-probe.sh
```

```text
$ cat url-probe/run-probe.sh
#!/bin/sh

python3 /app/probe.py /config/urls.txt
```

```text
$ chmod +x url-probe/run-probe.sh
```

```text
$ nano url-probe/Dockerfile
```

```text
$ cat url-probe/Dockerfile
FROM python:3.12-alpine

RUN adduser -D probe

WORKDIR /app

COPY probe.py run-probe.sh ./
COPY crontab /etc/crontabs/probe

RUN chmod +x run-probe.sh

CMD ["crond", "-f", "-d", "8"]
```

```text
$ docker build -t url-probe:hw ./url-probe
[+] Building 1.5s (11/11) FINISHED                               docker:default
 => [internal] load build definition from Dockerfile                       0.0s
 => => transferring dockerfile: 293B                                       0.0s
 => [internal] load metadata for docker.io/library/python:3.12-alpine      1.0s
 => [internal] load .dockerignore                                          0.0s
 => => transferring context: 2B                                            0.0s
 => [1/6] FROM docker.io/library/python:3.12-alpine@sha256:b658b05ce9c4eb  0.0s
 => => resolve docker.io/library/python:3.12-alpine@sha256:b658b05ce9c4eb  0.0s
 => [internal] load build context                                          0.0s
 => => transferring context: 402B                                          0.0s
 => CACHED [2/6] RUN adduser -D probe                                      0.0s
 => CACHED [3/6] WORKDIR /app                                              0.0s
 => [4/6] COPY probe.py run-probe.sh ./                                    0.0s
 => [5/6] COPY crontab /etc/crontabs/probe                                 0.0s
 => [6/6] RUN chmod +x run-probe.sh                                        0.1s
 => exporting to image                                                     0.2s
 => => exporting layers                                                    0.1s
 => => exporting manifest sha256:9cb984a42febac0c79c766dfee6e3f08257eb006  0.0s
 => => exporting config sha256:db1b7a8294f5bec40f5183a66004c3ccd99cf9ab07  0.0s
 => => exporting attestation manifest sha256:b297c65ed949d7b13c5bbf28a26d  0.0s
 => => exporting manifest list sha256:1fc6c7e42513d5549bc5242fcc9581e62ef  0.0s
 => => naming to docker.io/library/url-probe:hw                            0.0s
 => => unpacking to docker.io/library/url-probe:hw                         0.0s
```

```text
$ docker run -d --name url-probe-test -v "$PWD/url-probe:/config:ro,Z" url-probe:hw
7e833361aecac7865cfa703556d8d445ed5dbd053f9f6cfb43d5627d1428a27d
```

```text
$ docker ps -a --filter name=url-probe-test
CONTAINER ID   IMAGE          COMMAND           CREATED          STATUS          PORTS     NAMES
7e833361aeca   url-probe:hw   "crond -f -d 8"   34 seconds ago   Up 33 seconds             url-probe-test
```

```text
$ docker logs url-probe-test
crond: crond (busybox 1.37.0) started, log level 8
crond: USER probe pid   7 cmd /app/run-probe.sh
OK   200  0.118s https://example.com
FAIL 404  0.124s https://example.com/no-such-page
http://no-such-host.invalid: <urlopen error [Errno -2] Name does not resolve>
FAIL   0  0.044s http://no-such-host.invalid
http://10.255.255.1: <urlopen error timed out>
FAIL   0  5.005s http://10.255.255.1
# HELP probe_success 1, если получен ответ с кодом 2xx/3xx, иначе 0
# TYPE probe_success gauge
probe_success{url="https://example.com"} 1
probe_success{url="https://example.com/no-such-page"} 0
probe_success{url="http://no-such-host.invalid"} 0
probe_success{url="http://10.255.255.1"} 0
# HELP probe_http_status_code Код ответа HTTP, 0 - ответа нет
# TYPE probe_http_status_code gauge
probe_http_status_code{url="https://example.com"} 200
probe_http_status_code{url="https://example.com/no-such-page"} 404
probe_http_status_code{url="http://no-such-host.invalid"} 0
probe_http_status_code{url="http://10.255.255.1"} 0
# HELP probe_duration_seconds Сколько заняла проверка
# TYPE probe_duration_seconds gauge
probe_duration_seconds{url="https://example.com"} 0.118
probe_duration_seconds{url="https://example.com/no-such-page"} 0.124
probe_duration_seconds{url="http://no-such-host.invalid"} 0.044
probe_duration_seconds{url="http://10.255.255.1"} 5.005
# HELP probe_last_run_timestamp_seconds Время последнего запуска
# TYPE probe_last_run_timestamp_seconds gauge
probe_last_run_timestamp_seconds 1792932906
```

```text
$ docker logs url-probe-test
crond: crond (busybox 1.37.0) started, log level 8
crond: USER probe pid   7 cmd /app/run-probe.sh
OK   200  0.118s https://example.com
FAIL 404  0.124s https://example.com/no-such-page
http://no-such-host.invalid: <urlopen error [Errno -2] Name does not resolve>
FAIL   0  0.044s http://no-such-host.invalid
http://10.255.255.1: <urlopen error timed out>
FAIL   0  5.005s http://10.255.255.1
# HELP probe_success 1, если получен ответ с кодом 2xx/3xx, иначе 0
# TYPE probe_success gauge
probe_success{url="https://example.com"} 1
probe_success{url="https://example.com/no-such-page"} 0
probe_success{url="http://no-such-host.invalid"} 0
probe_success{url="http://10.255.255.1"} 0
# HELP probe_http_status_code Код ответа HTTP, 0 - ответа нет
# TYPE probe_http_status_code gauge
probe_http_status_code{url="https://example.com"} 200
probe_http_status_code{url="https://example.com/no-such-page"} 404
probe_http_status_code{url="http://no-such-host.invalid"} 0
probe_http_status_code{url="http://10.255.255.1"} 0
# HELP probe_duration_seconds Сколько заняла проверка
# TYPE probe_duration_seconds gauge
probe_duration_seconds{url="https://example.com"} 0.118
probe_duration_seconds{url="https://example.com/no-such-page"} 0.124
probe_duration_seconds{url="http://no-such-host.invalid"} 0.044
probe_duration_seconds{url="http://10.255.255.1"} 5.005
# HELP probe_last_run_timestamp_seconds Время последнего запуска
# TYPE probe_last_run_timestamp_seconds gauge
probe_last_run_timestamp_seconds 1792932906
crond: USER probe pid   9 cmd /app/run-probe.sh
OK   200  0.127s https://example.com
FAIL 404  0.096s https://example.com/no-such-page
http://no-such-host.invalid: <urlopen error [Errno -2] Name does not resolve>
FAIL   0  0.045s http://no-such-host.invalid
http://10.255.255.1: <urlopen error timed out>
FAIL   0  5.004s http://10.255.255.1
# HELP probe_success 1, если получен ответ с кодом 2xx/3xx, иначе 0
# TYPE probe_success gauge
probe_success{url="https://example.com"} 1
probe_success{url="https://example.com/no-such-page"} 0
probe_success{url="http://no-such-host.invalid"} 0
probe_success{url="http://10.255.255.1"} 0
# HELP probe_http_status_code Код ответа HTTP, 0 - ответа нет
# TYPE probe_http_status_code gauge
probe_http_status_code{url="https://example.com"} 200
probe_http_status_code{url="https://example.com/no-such-page"} 404
probe_http_status_code{url="http://no-such-host.invalid"} 0
probe_http_status_code{url="http://10.255.255.1"} 0
# HELP probe_duration_seconds Сколько заняла проверка
# TYPE probe_duration_seconds gauge
probe_duration_seconds{url="https://example.com"} 0.127
probe_duration_seconds{url="https://example.com/no-such-page"} 0.096
probe_duration_seconds{url="http://no-such-host.invalid"} 0.045
probe_duration_seconds{url="http://10.255.255.1"} 5.004
# HELP probe_last_run_timestamp_seconds Время последнего запуска
# TYPE probe_last_run_timestamp_seconds gauge
```

## 4. Таймаут через cron и смена URL без рестарта

Перезапустил тестовый контейнер с PROBE_TIMEOUT=2, проверил длительность запроса. Затем добавил URL в файл, дождался новой проверки и сравнил StartedAt до и после.

```text
$ docker rm -f url-probe-test
url-probe-test
```

```text
$ docker run -d --name url-probe-test -e PROBE_TIMEOUT=2 -v "$PWD/url-probe:/config:ro,Z" url-probe:hw
688e87dc0dec2a390eff6dcb52cde15ef1d60d1e412db4f602461f48c0c113b5
```

```text
$ docker logs url-probe-test
crond: crond (busybox 1.37.0) started, log level 8
crond: USER probe pid   7 cmd /app/run-probe.sh
OK   200  0.127s https://example.com
FAIL 404  0.106s https://example.com/no-such-page
http://no-such-host.invalid: <urlopen error [Errno -2] Name does not resolve>
FAIL   0  0.039s http://no-such-host.invalid
http://10.255.255.1: <urlopen error timed out>
FAIL   0  2.002s http://10.255.255.1
# HELP probe_success 1, если получен ответ с кодом 2xx/3xx, иначе 0
# TYPE probe_success gauge
probe_success{url="https://example.com"} 1
probe_success{url="https://example.com/no-such-page"} 0
probe_success{url="http://no-such-host.invalid"} 0
probe_success{url="http://10.255.255.1"} 0
# HELP probe_http_status_code Код ответа HTTP, 0 - ответа нет
# TYPE probe_http_status_code gauge
probe_http_status_code{url="https://example.com"} 200
probe_http_status_code{url="https://example.com/no-such-page"} 404
probe_http_status_code{url="http://no-such-host.invalid"} 0
probe_http_status_code{url="http://10.255.255.1"} 0
# HELP probe_duration_seconds Сколько заняла проверка
# TYPE probe_duration_seconds gauge
probe_duration_seconds{url="https://example.com"} 0.127
probe_duration_seconds{url="https://example.com/no-such-page"} 0.106
probe_duration_seconds{url="http://no-such-host.invalid"} 0.039
probe_duration_seconds{url="http://10.255.255.1"} 2.002
# HELP probe_last_run_timestamp_seconds Время последнего запуска
# TYPE probe_last_run_timestamp_seconds gauge
probe_last_run_timestamp_seconds 1792933083
```

```text
$ docker inspect -f '{{.State.StartedAt}}' url-probe-test
2026-10-25T12:57:43.464355596Z
```

```text
$ echo 'https://example.org' >> url-probe/urls.txt
```

```text
$ docker logs --tail 35 url-probe-test
probe_duration_seconds{url="https://example.com"} 0.127
probe_duration_seconds{url="https://example.com/no-such-page"} 0.106
probe_duration_seconds{url="http://no-such-host.invalid"} 0.039
probe_duration_seconds{url="http://10.255.255.1"} 2.002
# HELP probe_last_run_timestamp_seconds Время последнего запуска
# TYPE probe_last_run_timestamp_seconds gauge
probe_last_run_timestamp_seconds 1792933083
crond: USER probe pid   9 cmd /app/run-probe.sh
OK   200  0.132s https://example.com
FAIL 404  0.099s https://example.com/no-such-page
http://no-such-host.invalid: <urlopen error [Errno -2] Name does not resolve>
FAIL   0  0.037s http://no-such-host.invalid
http://10.255.255.1: <urlopen error timed out>
FAIL   0  2.002s http://10.255.255.1
# HELP probe_success 1, если получен ответ с кодом 2xx/3xx, иначе 0
# TYPE probe_success gauge
probe_success{url="https://example.com"} 1
probe_success{url="https://example.com/no-such-page"} 0
probe_success{url="http://no-such-host.invalid"} 0
probe_success{url="http://10.255.255.1"} 0
# HELP probe_http_status_code Код ответа HTTP, 0 - ответа нет
# TYPE probe_http_status_code gauge
probe_http_status_code{url="https://example.com"} 200
probe_http_status_code{url="https://example.com/no-such-page"} 404
probe_http_status_code{url="http://no-such-host.invalid"} 0
probe_http_status_code{url="http://10.255.255.1"} 0
# HELP probe_duration_seconds Сколько заняла проверка
# TYPE probe_duration_seconds gauge
probe_duration_seconds{url="https://example.com"} 0.132
probe_duration_seconds{url="https://example.com/no-such-page"} 0.099
probe_duration_seconds{url="http://no-such-host.invalid"} 0.037
probe_duration_seconds{url="http://10.255.255.1"} 2.002
# HELP probe_last_run_timestamp_seconds Время последнего запуска
# TYPE probe_last_run_timestamp_seconds gauge
probe_last_run_timestamp_seconds 1792933143
```

```text
$ docker exec url-probe-test cat /config/urls.txt
# Один URL на строку. Пустые строки и строки с # пропускаются.
https://example.com
https://example.com/no-such-page
http://no-such-host.invalid
http://10.255.255.1
https://example.org
```

```text
$ docker logs --tail 45 url-probe-test
probe_http_status_code{url="http://no-such-host.invalid"} 0
probe_http_status_code{url="http://10.255.255.1"} 0
# HELP probe_duration_seconds Сколько заняла проверка
# TYPE probe_duration_seconds gauge
probe_duration_seconds{url="https://example.com"} 0.132
probe_duration_seconds{url="https://example.com/no-such-page"} 0.099
probe_duration_seconds{url="http://no-such-host.invalid"} 0.037
probe_duration_seconds{url="http://10.255.255.1"} 2.002
# HELP probe_last_run_timestamp_seconds Время последнего запуска
# TYPE probe_last_run_timestamp_seconds gauge
probe_last_run_timestamp_seconds 1792933143
crond: USER probe pid  11 cmd /app/run-probe.sh
crond: USER root pid  12 cmd run-parts /etc/periodic/15min
crond: USER root pid  14 cmd run-parts /etc/periodic/hourly
OK   200  0.147s https://example.com
FAIL 404  0.117s https://example.com/no-such-page
http://no-such-host.invalid: <urlopen error [Errno -2] Name does not resolve>
FAIL   0  0.034s http://no-such-host.invalid
http://10.255.255.1: <urlopen error timed out>
FAIL   0  2.002s http://10.255.255.1
OK   200  0.178s https://example.org
# HELP probe_success 1, если получен ответ с кодом 2xx/3xx, иначе 0
# TYPE probe_success gauge
probe_success{url="https://example.com"} 1
probe_success{url="https://example.com/no-such-page"} 0
probe_success{url="http://no-such-host.invalid"} 0
probe_success{url="http://10.255.255.1"} 0
probe_success{url="https://example.org"} 1
# HELP probe_http_status_code Код ответа HTTP, 0 - ответа нет
# TYPE probe_http_status_code gauge
probe_http_status_code{url="https://example.com"} 200
probe_http_status_code{url="https://example.com/no-such-page"} 404
probe_http_status_code{url="http://no-such-host.invalid"} 0
probe_http_status_code{url="http://10.255.255.1"} 0
probe_http_status_code{url="https://example.org"} 200
# HELP probe_duration_seconds Сколько заняла проверка
# TYPE probe_duration_seconds gauge
probe_duration_seconds{url="https://example.com"} 0.147
probe_duration_seconds{url="https://example.com/no-such-page"} 0.117
probe_duration_seconds{url="http://no-such-host.invalid"} 0.034
probe_duration_seconds{url="http://10.255.255.1"} 2.002
probe_duration_seconds{url="https://example.org"} 0.178
# HELP probe_last_run_timestamp_seconds Время последнего запуска
# TYPE probe_last_run_timestamp_seconds gauge
probe_last_run_timestamp_seconds 1792933203
```

```text
$ docker inspect -f '{{.State.StartedAt}}' url-probe-test
2026-10-25T12:57:43.464355596Z
```

## 5. Автоматическое восстановление контейнера

Настроил unless-stopped. При ручном docker kill контейнер остался остановленным, поэтому отдельно проверил аварийное завершение основного процесса crond. После него Docker сам поднял контейнер.

```text
$ docker inspect -f '{{.HostConfig.RestartPolicy.Name}}' url-probe-test
no
```

```text
$ docker update --restart unless-stopped url-probe-test
url-probe-test
```

```text
$ docker inspect -f '{{.HostConfig.RestartPolicy.Name}}' url-probe-test
unless-stopped
```

```text
$ docker kill url-probe-test
url-probe-test
```

```text
$ docker ps -a --filter name=url-probe-test
CONTAINER ID   IMAGE          COMMAND           CREATED         STATUS                        PORTS     NAMES
688e87dc0dec   url-probe:hw   "crond -f -d 8"   8 minutes ago   Exited (137) 27 seconds ago             url-probe-test
```

```text
$ docker start url-probe-test
url-probe-test
```

```text
$ docker inspect -f '{{.State.Pid}}' url-probe-test
1959981
```

```text
$ ps -p 1959981 -o pid,comm,args
    PID COMMAND         COMMAND
1959981 crond           crond -f -d 8
```

```text
$ sudo kill -9 1959981
[sudo] пароль для tadzhnahal:
```

```text
$ docker inspect -f 'Status={{.State.Status}} RestartCount={{.RestartCount}} StartedAt={{.State.StartedAt}}' url-probe-test
Status=running RestartCount=1 StartedAt=2026-10-25T13:10:03.019784796Z
```

```text
$ docker rm -f url-probe-test
url-probe-test
```

## 6. Конфигурация Prometheus и правила алерта

Добавил конфигурацию сбора метрик раз в 15 секунд и правило UrlDown с for: 1m. Проверил оба файла через promtool.

```text
$ mkdir prometheus
```

```text
$ nano prometheus/prometheus.yml
```

```text
$ cat prometheus/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - /etc/prometheus/rules.yml

scrape_configs:
  - job_name: node
    static_configs:
      - targets: [node-exporter:9100]
```

```text
$ nano prometheus/rules.yml
```

```text
$ cat prometheus/rules.yml
groups:
  - name: url-probe
    rules:
      - alert: UrlDown
        expr: probe_success == 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "URL недоступен больше минуты"
```

```text
$ docker run --rm -v "$PWD/prometheus:/etc/prometheus:ro,Z" --entrypoint promtool prom/prometheus:v3.5.0 check rules /etc/prometheus/rules.yml
Unable to find image 'prom/prometheus:v3.5.0' locally
v3.5.0: Pulling from prom/prometheus
2ee6cb77bebd: Pull complete
097a69c6efe6: Pull complete
fd1d3a5a5f79: Pull complete
a4e782810d03: Pull complete
208063e2dcbb: Pull complete
76619c1908eb: Pull complete
1617e25568b2: Pull complete
2dfc70ad9941: Pull complete
5e4c02bc6754: Pull complete
9fa9226be034: Pull complete
Digest: sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996
Status: Downloaded newer image for prom/prometheus:v3.5.0
Checking /etc/prometheus/rules.yml
  SUCCESS: 1 rules found
```

```text
$ docker run --rm -v "$PWD/prometheus:/etc/prometheus:ro,Z" --entrypoint promtool prom/prometheus:v3.5.0 check config /etc/prometheus/prometheus.yml
Checking /etc/prometheus/prometheus.yml
  SUCCESS: 1 rule files found
 SUCCESS: /etc/prometheus/prometheus.yml is valid prometheus config file syntax

Checking /etc/prometheus/rules.yml
  SUCCESS: 1 rules found
```

## 7. Запуск всего стека через Compose

Добавил url-probe, Node Exporter, Prometheus и тестовый Nginx. Порт 9090 оказался занят sing-box, поэтому внешний порт Prometheus поменял на 9091. Внутри Docker используется прежний 9090.

```text
$ nano compose.yaml
```

```text
$ cat compose.yaml
services:
  url-probe:
    build: ./url-probe
    environment:
      PROBE_TIMEOUT: "2"
      PROBE_OUTPUT_DIR: /textfile
    volumes:
      - ./url-probe:/config:ro,Z
      - textfile:/textfile
    command: /bin/sh -c "chown probe:probe /textfile && exec crond -f -d 8"
    restart: unless-stopped

  node-exporter:
    image: prom/node-exporter:v1.9.1
    command:
      - --collector.textfile.directory=/textfile
    volumes:
      - textfile:/textfile:ro
    restart: unless-stopped

  prometheus:
    image: prom/prometheus:v3.5.0
    volumes:
      - ./prometheus:/etc/prometheus:ro,Z
    ports:
      - "127.0.0.1:9090:9090"
    restart: unless-stopped

  test-site:
    image: nginx:alpine
    restart: unless-stopped

volumes:
  textfile:
```

```text
$ docker compose config --quiet
```

```text
$ docker compose up -d
[+] up 2/2
 ✔ Image prom/node-exporter:v1.9.1 Pulled                                  24.8s
[+] Building 1.4s (13/13) FINISHED
 => [internal] load local bake definitions                                 0.0s
 => => reading from stdin 608B                                             0.0s
 => [internal] load build definition from Dockerfile                       0.0s
 => => transferring dockerfile: 292B                                       0.0s
 => [internal] load metadata for docker.io/library/python:3.12-alpine      1.0s
 => [internal] load .dockerignore                                          0.0s
 => => transferring context: 2B                                            0.0s
 => [1/6] FROM docker.io/library/python:3.12-alpine@sha256:b658b05ce9c4eb  0.0s
 => => resolve docker.io/library/python:3.12-alpine@sha256:b658b05ce9c4eb  0.0s
 => [internal] load build context                                          0.0s
 => => transferring context: 297B                                          0.0s
 => CACHED [2/6] RUN adduser -D probe                                      0.0s
 => CACHED [3/6] WORKDIR /app                                              0.0s
 => CACHED [4/6] COPY probe.py run-probe.sh ./                             0.0s
 => CACHED [5/6] COPY crontab /etc/crontabs/probe                          0.0s
 => CACHED [6/6] RUN chmod +x run-probe.sh                                 0.0s
 => exporting to image                                                     0.1s
 => => exporting layers                                                    0.0s
 => => exporting manifest sha256:141c1da1d4c22604b47026452fdbb259b71b3e74  0.0s
 => => exporting config sha256:49b1f5a044c27f42c19562e3e14ba0c440a70ee5d0  0.0s
 => => exporting attestation manifest sha256:53e2afd5b5062c91e078c3da82c6  0.0s
 => => exporting manifest list sha256:353198469b81c2107c04212b4d344419572  0.0s
 => => naming to docker.io/library/bonus-hw-url-probe:latest               0.0s
[+] up 8/9acking to docker.io/library/bonus-hw-url-probe:latest            0.0s
 ✔ Image prom/node-exporter:v1.9.1    Pulled                               24.8s
 ✔ Image bonus-hw-url-probe           Built                                 1.5s
 ✔ Network bonus-hw_default           Created                               0.1s
 ✔ Volume bonus-hw_textfile           Created                               0.0s
 ✔ Container bonus-hw-url-probe-1     Started                               0.4s
 ⠴ Container bonus-hw-prometheus-1    Starting                              0.5s
 ✔ Container bonus-hw-node-exporter-1 Started                               0.4s
 ✔ Container bonus-hw-test-site-1     Started                               0.4s
Error response from daemon: failed to set up container networking: driver failed programming external connectivity on endpoint bonus-hw-prometheus-1 (0ede3a5b261ceb987dae9431f3bdacd6fe2aaefc5dbb6cb3f89d94b5e89e6be6): failed to bind host port 127.0.0.1:9090/tcp: address already in use
```

```text
$ sudo ss -ltnp 'sport = :9090'
[sudo] пароль для tadzhnahal:
State     Recv-Q    Send-Q       Local Address:Port        Peer Address:Port    Process
LISTEN    0         4096             127.0.0.1:9090             0.0.0.0:*        users:(("sing-box-lx",pid=2005239,fd=10))
```

```text
$ ss -ltn 'sport = :9091'
State    Recv-Q    Send-Q       Local Address:Port         Peer Address:Port
```

```text
$ nano compose.yaml
```

```text
$ docker compose up -d
[+] up 4/4
 ✔ Container bonus-hw-test-site-1     Running                               0.0s
 ✔ Container bonus-hw-url-probe-1     Running                               0.0s
 ✔ Container bonus-hw-node-exporter-1 Running                               0.0s
 ✔ Container bonus-hw-prometheus-1    Started                               0.5s
```

```text
$ docker compose ps
NAME                       IMAGE                       COMMAND                  SERVICE         CREATED          STATUS          PORTS
bonus-hw-node-exporter-1   prom/node-exporter:v1.9.1   "/bin/node_exporter …"   node-exporter   4 minutes ago    Up 4 minutes    9100/tcp
bonus-hw-prometheus-1      prom/prometheus:v3.5.0      "/bin/prometheus --c…"   prometheus      31 seconds ago   Up 30 seconds   127.0.0.1:9091->9090/tcp
bonus-hw-test-site-1       nginx:alpine                "/docker-entrypoint.…"   test-site       4 minutes ago    Up 4 minutes    80/tcp
bonus-hw-url-probe-1       bonus-hw-url-probe          "/bin/sh -c 'chown p…"   url-probe       4 minutes ago    Up 4 minutes
```

## 8. Проверка метрик и textfile collector

Проверил файл url_probe.prom в общем томе, наличие probe_success для URL в Prometheus и отсутствие ошибок чтения файла у Node Exporter.

```text
$ docker compose exec url-probe cat /textfile/url_probe.prom
# HELP probe_success 1, если получен ответ с кодом 2xx/3xx, иначе 0
# TYPE probe_success gauge
probe_success{url="https://example.com"} 1
probe_success{url="https://example.com/no-such-page"} 0
probe_success{url="http://no-such-host.invalid"} 0
probe_success{url="http://10.255.255.1"} 0
probe_success{url="https://example.org"} 1
# HELP probe_http_status_code Код ответа HTTP, 0 - ответа нет
# TYPE probe_http_status_code gauge
probe_http_status_code{url="https://example.com"} 200
probe_http_status_code{url="https://example.com/no-such-page"} 404
probe_http_status_code{url="http://no-such-host.invalid"} 0
probe_http_status_code{url="http://10.255.255.1"} 0
probe_http_status_code{url="https://example.org"} 200
# HELP probe_duration_seconds Сколько заняла проверка
# TYPE probe_duration_seconds gauge
probe_duration_seconds{url="https://example.com"} 0.122
probe_duration_seconds{url="https://example.com/no-such-page"} 0.118
probe_duration_seconds{url="http://no-such-host.invalid"} 0.001
probe_duration_seconds{url="http://10.255.255.1"} 2.002
probe_duration_seconds{url="https://example.org"} 0.105
# HELP probe_last_run_timestamp_seconds Время последнего запуска
# TYPE probe_last_run_timestamp_seconds gauge
probe_last_run_timestamp_seconds 1792934943
```

```text
$ curl -s 'http://127.0.0.1:9091/api/v1/query?query=probe_success'
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {
          "__name__": "probe_success",
          "instance": "node-exporter:9100",
          "job": "node",
          "url": "http://10.255.255.1"
        },
        "value": [
          1792934999.553,
          "0"
        ]
      },
      {
        "metric": {
          "__name__": "probe_success",
          "instance": "node-exporter:9100",
          "job": "node",
          "url": "http://no-such-host.invalid"
        },
        "value": [
          1792934999.553,
          "0"
        ]
      },
      {
        "metric": {
          "__name__": "probe_success",
          "instance": "node-exporter:9100",
          "job": "node",
          "url": "https://example.com"
        },
        "value": [
          1792934999.553,
          "1"
        ]
      },
      {
        "metric": {
          "__name__": "probe_success",
          "instance": "node-exporter:9100",
          "job": "node",
          "url": "https://example.com/no-such-page"
        },
        "value": [
          1792934999.553,
          "0"
        ]
      },
      {
        "metric": {
          "__name__": "probe_success",
          "instance": "node-exporter:9100",
          "job": "node",
          "url": "https://example.org"
        },
        "value": [
          1792934999.553,
          "1"
        ]
      }
    ]
  }
}
```

```text
$ curl -s 'http://127.0.0.1:9091/api/v1/query?query=node_textfile_scrape_error'
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {
          "__name__": "node_textfile_scrape_error",
          "instance": "node-exporter:9100",
          "job": "node"
        },
        "value": [
          1792935037.955,
          "0"
        ]
      }
    ]
  }
}
```

```text
$ echo 'http://test-site' >> url-probe/urls.txt
```

```text
$ docker compose logs --tail 25 url-probe
url-probe-1  | crond: USER probe pid  18 cmd /app/run-probe.sh
url-probe-1  | OK   200  0.122s https://example.com
url-probe-1  | FAIL 404  0.118s https://example.com/no-such-page
url-probe-1  | http://no-such-host.invalid: <urlopen error [Errno -2] Name does not resolve>
url-probe-1  | FAIL   0  0.001s http://no-such-host.invalid
url-probe-1  | http://10.255.255.1: <urlopen error timed out>
url-probe-1  | FAIL   0  2.002s http://10.255.255.1
url-probe-1  | OK   200  0.105s https://example.org
url-probe-1  | crond: USER probe pid  27 cmd /app/run-probe.sh
url-probe-1  | crond: USER root pid  28 cmd run-parts /etc/periodic/15min
url-probe-1  | OK   200  0.158s https://example.com
url-probe-1  | FAIL 404  0.114s https://example.com/no-such-page
url-probe-1  | http://no-such-host.invalid: <urlopen error [Errno -2] Name does not resolve>
url-probe-1  | FAIL   0  0.003s http://no-such-host.invalid
url-probe-1  | http://10.255.255.1: <urlopen error timed out>
url-probe-1  | FAIL   0  2.003s http://10.255.255.1
url-probe-1  | OK   200  0.183s https://example.org
url-probe-1  | crond: USER probe pid  30 cmd /app/run-probe.sh
url-probe-1  | OK   200  0.186s https://example.com
url-probe-1  | FAIL 404  0.116s https://example.com/no-such-page
url-probe-1  | http://no-such-host.invalid: <urlopen error [Errno -2] Name does not resolve>
url-probe-1  | FAIL   0  0.001s http://no-such-host.invalid
url-probe-1  | http://10.255.255.1: <urlopen error timed out>
url-probe-1  | FAIL   0  2.002s http://10.255.255.1
url-probe-1  | OK   200  0.592s https://example.org
```

```text
$ docker compose exec url-probe cat /config/urls.txt
# Один URL на строку. Пустые строки и строки с # пропускаются.
https://example.com
https://example.com/no-such-page
http://no-such-host.invalid
http://10.255.255.1
https://example.org
http://test-site
```

```text
$ docker compose logs --tail 20 url-probe
url-probe-1  | http://10.255.255.1: <urlopen error timed out>
url-probe-1  | FAIL   0  2.003s http://10.255.255.1
url-probe-1  | OK   200  0.183s https://example.org
url-probe-1  | crond: USER probe pid  30 cmd /app/run-probe.sh
url-probe-1  | OK   200  0.186s https://example.com
url-probe-1  | FAIL 404  0.116s https://example.com/no-such-page
url-probe-1  | http://no-such-host.invalid: <urlopen error [Errno -2] Name does not resolve>
url-probe-1  | FAIL   0  0.001s http://no-such-host.invalid
url-probe-1  | http://10.255.255.1: <urlopen error timed out>
url-probe-1  | FAIL   0  2.002s http://10.255.255.1
url-probe-1  | OK   200  0.592s https://example.org
url-probe-1  | crond: USER probe pid  32 cmd /app/run-probe.sh
url-probe-1  | OK   200  0.104s https://example.com
url-probe-1  | FAIL 404  0.116s https://example.com/no-such-page
url-probe-1  | http://no-such-host.invalid: <urlopen error [Errno -2] Name does not resolve>
url-probe-1  | FAIL   0  0.001s http://no-such-host.invalid
url-probe-1  | http://10.255.255.1: <urlopen error timed out>
url-probe-1  | FAIL   0  2.002s http://10.255.255.1
url-probe-1  | OK   200  0.111s https://example.org
url-probe-1  | OK   200  0.001s http://test-site
```

```text
$ curl -s 'http://127.0.0.1:9091/api/v1/query?query=probe_success'
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {
          "__name__": "probe_success",
          "instance": "node-exporter:9100",
          "job": "node",
          "url": "http://10.255.255.1"
        },
        "value": [
          1792935206.329,
          "0"
        ]
      },
      {
        "metric": {
          "__name__": "probe_success",
          "instance": "node-exporter:9100",
          "job": "node",
          "url": "http://no-such-host.invalid"
        },
        "value": [
          1792935206.329,
          "0"
        ]
      },
      {
        "metric": {
          "__name__": "probe_success",
          "instance": "node-exporter:9100",
          "job": "node",
          "url": "https://example.com"
        },
        "value": [
          1792935206.329,
          "1"
        ]
      },
      {
        "metric": {
          "__name__": "probe_success",
          "instance": "node-exporter:9100",
          "job": "node",
          "url": "https://example.com/no-such-page"
        },
        "value": [
          1792935206.329,
          "0"
        ]
      },
      {
        "metric": {
          "__name__": "probe_success",
          "instance": "node-exporter:9100",
          "job": "node",
          "url": "https://example.org"
        },
        "value": [
          1792935206.329,
          "1"
        ]
      },
      {
        "metric": {
          "__name__": "probe_success",
          "instance": "node-exporter:9100",
          "job": "node",
          "url": "http://test-site"
        },
        "value": [
          1792935206.329,
          "1"
        ]
      }
    ]
  }
}
```

## 9. Проверка алерта при остановке сайта

Сначала проверил, что для работающего тестового Nginx нет алерта. Затем остановил Nginx: после очередной проверки cron метрика стала равна 0, а алерт перешёл сначала в pending, потом в firing.

```text
$ curl -s 'http://127.0.0.1:9091/api/v1/alerts' | python3 -m json.tool
{
    "status": "success",
    "data": {
        "alerts": [
            {
                "labels": {
                    "alertname": "UrlDown",
                    "instance": "node-exporter:9100",
                    "job": "node",
                    "severity": "warning",
                    "url": "http://10.255.255.1"
                },
                "annotations": {
                    "summary": "URL \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d \u0431\u043e\u043b\u044c\u0448\u0435 \u043c\u0438\u043d\u0443\u0442\u044b"
                },
                "state": "firing",
                "activeAt": "2026-10-25T13:28:22.658669594Z",
                "value": "0e+00"
            },
            {
                "labels": {
                    "alertname": "UrlDown",
                    "instance": "node-exporter:9100",
                    "job": "node",
                    "severity": "warning",
                    "url": "http://no-such-host.invalid"
                },
                "annotations": {
                    "summary": "URL \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d \u0431\u043e\u043b\u044c\u0448\u0435 \u043c\u0438\u043d\u0443\u0442\u044b"
                },
                "state": "firing",
                "activeAt": "2026-10-25T13:28:22.658669594Z",
                "value": "0e+00"
            },
            {
                "labels": {
                    "alertname": "UrlDown",
                    "instance": "node-exporter:9100",
                    "job": "node",
                    "severity": "warning",
                    "url": "https://example.com/no-such-page"
                },
                "annotations": {
                    "summary": "URL \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d \u0431\u043e\u043b\u044c\u0448\u0435 \u043c\u0438\u043d\u0443\u0442\u044b"
                },
                "state": "firing",
                "activeAt": "2026-10-25T13:28:22.658669594Z",
                "value": "0e+00"
            }
        ]
    }
}
```

```text
$ docker compose stop test-site
[+] stop 1/1
```

```text
$ curl -sG --data-urlencode 'query=probe_success{url="http://test-site"}' http://127.0.0.1:9091/api/v1/query | python3 -m json.tool
{
    "status": "success",
    "data": {
        "resultType": "vector",
        "result": [
            {
                "metric": {
                    "__name__": "probe_success",
                    "instance": "node-exporter:9100",
                    "job": "node",
                    "url": "http://test-site"
                },
                "value": [
                    1792935424.306,
                    "1"
                ]
            }
        ]
    }
}
```

```text
$ docker compose logs --tail 20 url-probe
url-probe-1  | OK   200  0.001s http://test-site
url-probe-1  | crond: USER probe pid  46 cmd /app/run-probe.sh
url-probe-1  | OK   200  0.117s https://example.com
url-probe-1  | FAIL 404  0.182s https://example.com/no-such-page
url-probe-1  | http://no-such-host.invalid: <urlopen error [Errno -2] Name does not resolve>
url-probe-1  | FAIL   0  0.001s http://no-such-host.invalid
url-probe-1  | http://10.255.255.1: <urlopen error timed out>
url-probe-1  | FAIL   0  2.002s http://10.255.255.1
url-probe-1  | OK   200  0.167s https://example.org
url-probe-1  | OK   200  0.001s http://test-site
url-probe-1  | crond: USER probe pid  48 cmd /app/run-probe.sh
url-probe-1  | OK   200  0.188s https://example.com
url-probe-1  | FAIL 404  0.115s https://example.com/no-such-page
url-probe-1  | http://no-such-host.invalid: <urlopen error [Errno -2] Name does not resolve>
url-probe-1  | FAIL   0  0.001s http://no-such-host.invalid
url-probe-1  | http://10.255.255.1: <urlopen error timed out>
url-probe-1  | FAIL   0  2.002s http://10.255.255.1
url-probe-1  | OK   200  0.111s https://example.org
url-probe-1  | http://test-site: <urlopen error [Errno -2] Name does not resolve>
url-probe-1  | FAIL   0  1.222s http://test-site
```

```text
$ curl -sG --data-urlencode 'query=ALERTS{alertname="UrlDown",url="http://test-site"}' http://127.0.0.1:9091/api/v1/query | python3 -m json.tool
{
    "status": "success",
    "data": {
        "resultType": "vector",
        "result": [
            {
                "metric": {
                    "__name__": "ALERTS",
                    "alertname": "UrlDown",
                    "alertstate": "pending",
                    "instance": "node-exporter:9100",
                    "job": "node",
                    "severity": "warning",
                    "url": "http://test-site"
                },
                "value": [
                    1792935491.409,
                    "1"
                ]
            }
        ]
    }
}
```

```text
$ curl -sG --data-urlencode 'query=ALERTS{alertname="UrlDown",url="http://test-site"}' http://127.0.0.1:9091/api/v1/query | python3 -m json.tool
{
    "status": "success",
    "data": {
        "resultType": "vector",
        "result": [
            {
                "metric": {
                    "__name__": "ALERTS",
                    "alertname": "UrlDown",
                    "alertstate": "firing",
                    "instance": "node-exporter:9100",
                    "job": "node",
                    "severity": "warning",
                    "url": "http://test-site"
                },
                "value": [
                    1792935525.603,
                    "1"
                ]
            }
        ]
    }
}
```

## 10. Восстановление сайта и снятие алерта

Запустил Nginx обратно, убедился, что probe_success снова равен 1, а алерт для http://test-site исчез.

```text
$ docker compose start test-site
[+] start 1/1
 ✔ Container bonus-hw-test-site-1 Started                                   0.1s
```

```text
$ curl -sG --data-urlencode 'query=probe_success{url="http://test-site"}' http://127.0.0.1:9091/api/v1/query | python3 -m json.tool
{
    "status": "success",
    "data": {
        "resultType": "vector",
        "result": [
            {
                "metric": {
                    "__name__": "probe_success",
                    "instance": "node-exporter:9100",
                    "job": "node",
                    "url": "http://test-site"
                },
                "value": [
                    1792935702.374,
                    "1"
                ]
            }
        ]
    }
}
```

```text
$ curl -sG --data-urlencode 'query=ALERTS{alertname="UrlDown",url="http://test-site"}' http://127.0.0.1:9091/api/v1/query | python3 -m json.tool
{
    "status": "success",
    "data": {
        "resultType": "vector",
        "result": []
    }
}
```

## 11. Проверка размера образа

Повторно проверил размер образа после всех изменений.

```text
$ docker images bonus-hw-url-probe
                                                            i Info →   U  In Use
IMAGE                       ID             DISK USAGE   CONTENT SIZE   EXTRA
bonus-hw-url-probe:latest   353198469b81       74.7MB         18.1MB    U
```
