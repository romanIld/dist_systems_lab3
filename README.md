# Lab3: Распределенная очередь задач с выбором лидера (Raft)

Кластер из 3-5 узлов. Узлы выбирают лидера по упрощенному Raft (только leader
election и heartbeat, без репликации лога и записи на диск). Только лидер
принимает задачи от клиентов и раздает их узлам по round-robin. При падении
лидера кластер сам выбирает нового, новый лидер собирает задачи со всех узлов,
задачи не теряются. Подробности, схемы и результаты замеров - в
[REPORT.md](REPORT.md).

## Структура

```
cmd/
  raftnode/     узел кластера (один процесс - один узел)
  client/       консольный клиент: add / status / tasks
  scenario/     прогон сценариев 1-7 и замеры на отдельных процессах -> results/
internal/
  raft/         роли, currentTerm/votedFor/votesReceived, выборы, heartbeat, watchdog, журнал переходов
  queue/        задачи, round-robin, реестр задач, воркер, синхронизация при смене лидера
  api/          HTTP-эндпоинты (публичные и внутренние)
  node/         сборка узла: raft + queue + HTTP-сервер
  cluster/      кластер в одном процессе для интеграционных тестов + сами тесты сценариев
  httpx/        JSON поверх HTTP
  bg/           фоновые горутины с корректной остановкой
results/        результаты прогона cmd/scenario и вывод go test
```

## Сборка и запуск

Нужен Go 1.25+.

```powershell
go build -o bin/raftnode.exe ./cmd/raftnode
go build -o bin/client.exe ./cmd/client
```

Три узла в трех терминалах (адреса по умолчанию 127.0.0.1:8081-8083):

```powershell
bin/raftnode.exe -id 1
bin/raftnode.exe -id 2
bin/raftnode.exe -id 3
```

Флаги `raftnode`:

| Флаг | По умолчанию | Назначение |
| --- | --- | --- |
| `-id` | 1 | ID узла (положительное число) |
| `-peers` | `1=http://127.0.0.1:8081,2=...:8082,3=...:8083` | все узлы кластера, включая свой; для 5 узлов - 5 записей |
| `-listen` | порт из своего адреса | адрес прослушивания |
| `-heartbeat` | 50ms | интервал heartbeat |
| `-election-min`, `-election-max` | 150ms, 300ms | диапазон случайного election timeout |
| `-work-time` | 2s | время "выполнения" одной задачи воркером |
| `-dead-after` | 300ms | лидер считает узел упавшим без ответа на heartbeat дольше этого |
| `-seed` | время старта | seed генератора таймаутов |
| `-log-level` | info | debug покажет каждый RequestVote |

Клиент (по умолчанию знает адреса трех локальных узлов, сам переходит к лидеру
по 307):

```powershell
bin/client.exe status
bin/client.exe add -priority high "Обработать заказ 12345"
bin/client.exe tasks 2
```

Падение лидера вручную: найти лидера через `client status` и завершить его
процесс (Ctrl+C в его терминале или `Stop-Process`); через 150-300 мс
`client status` покажет нового лидера с большим термом.

### Docker compose

```powershell
docker compose up --build -d     # node1..node3, порты хоста 8081-8083
bin/client.exe status
docker compose stop node1        # падение узла
docker compose start node1       # возвращение с пустой памятью
docker compose down
```

Внутри сети узлы адресуют друг друга как `http://node1:8080` и т.д.

## API

| Эндпоинт | Метод | Назначение | Доступ |
| --- | --- | --- | --- |
| `/task` | POST | добавить задачу `{"data": "...", "priority": "..."}` | только лидер, иначе 307 с адресом лидера (503 если лидер неизвестен) |
| `/tasks/{node_id}` | GET | задачи узла | любой узел |
| `/status` | GET | роль, терм, лидер, голоса, состояние очереди | любой узел |
| `/internal/task` | POST | принять задачу от лидера | лидер -> узел |
| `/internal/tasks` | GET | задачи, которые узел держит в памяти (синхронизация) | новый лидер -> узел |
| `/internal/registry?since=N` | GET | реестр всех задач (изменения после версии N) | фолловер -> лидер |
| `/internal/task-status` | POST | отчет узла о смене статусов своих задач | узел -> лидер |
| `/internal/transitions` | GET | журнал переходов между состояниями | любой узел |
| `/raft/request-vote` | POST | RequestVote RPC | кандидат -> узел |
| `/raft/append-entries` | POST | AppendEntries RPC (heartbeat) | лидер -> узел |

Примеры запросов и ответов - в разделе 2 [REPORT.md](REPORT.md).

## Тесты

```powershell
go vet ./...
go test ./...                       # unit + интеграционные сценарии 1-7 (около 15 с)
go test -count=20 ./...             # проверка воспроизводимости
go test -v ./internal/cluster/      # журнал шагов каждого сценария
$env:LAB3_NODE_LOGS=1; go test -v ./internal/cluster/   # плюс логи узлов
```

Интеграционные тесты поднимают узлы в одном процессе на реальных сокетах
127.0.0.1; падение узла - остановка его HTTP-сервера и всех горутин.

## Замеры

```powershell
go run ./cmd/scenario               # пишет results/scenario.md, логи узлов - в results/logs
go run ./cmd/scenario -only main    # только сценарии 1-7
```

Параметры: `-runs` (повторы замеров выборов, 10), `-seq`, `-conc-tasks`,
`-conc`, `-big` (объемы нагрузки), `-work-time`, `-node-log-level`.

## Внешние библиотеки

Нет. Используется только стандартная библиотека Go:

| Задача | Чем решена |
| --- | --- |
| HTTP-сервер и маршрутизация `GET /tasks/{node_id}` | `net/http` (шаблоны маршрутов Go 1.22+) |
| RPC между узлами | HTTP + `encoding/json` вместо gRPC: задание допускает HTTP, кодогенерация не нужна |
| Логи | `log/slog` |
| Случайный election timeout | `math/rand/v2` (PCG с seed на узел) |
| Запуск и остановка процессов узлов в замерах | `os/exec` |
| Тесты | `testing` |
