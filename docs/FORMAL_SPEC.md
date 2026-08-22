# SFQ(D) Java Library — формальная спецификация

## 1. Статус, область и нормативные слова

Статус: нормативная спецификация последовательного поведения библиотеки и
linearization contract concurrent production API.

Теоретическая основа простым языком и ссылки на полные статьи находятся в
[THEORY.md](THEORY.md). Все решения, которых нет в публикациях, помечены как
**проектное решение**.

Слова **ДОЛЖЕН**, **НЕ ДОЛЖЕН**, **МОЖЕТ** имеют нормативный смысл. Публичные
имена Java-типов и методов здесь не фиксируются; фиксируется наблюдаемая
семантика public API и production implementation.

В область спецификации входят один scheduler instance и вызовы:

- `registerFlow`;
- `closeFlow`;
- `enqueue`;
- `cancel`;
- `capacityAvailable`, далее также `dispatch`;
- `complete`;
- `snapshot()` и `snapshot(flowHandle)`.

Scheduler выбирает jobs, но не исполняет их, не владеет executor или resource
pool и не делает callbacks.

## 2. Модель и конфигурация

### 2.1 Внешние сущности

**Проектное API/numeric решение.** Поддерживаемый домен входов:

- `FlowId` — произвольный ненулевой caller object с эквивалентностью по
  `equals`. Его `equals` и `hashCode` ДОЛЖНЫ оставаться стабильными, пока flow
  registered.
- `FlowHandle` — opaque capability конкретной регистрации flow. Enqueue
  принимает этот handle, а не raw `FlowId`/weight.
- `JobId` — произвольный ненулевой caller object с эквивалентностью по
  `equals`. Его `equals` и `hashCode` ДОЛЖНЫ оставаться стабильными, пока job
  live.
- `Payload` — произвольный ненулевой caller object. Scheduler его не
  интерпретирует.
- `cost` — supplied cost job, целое число `1..Long.MAX_VALUE`.
- `weight` — вес flow, целое число `1..Long.MAX_VALUE`.

`FlowId` и `JobId` ДОЛЖНЫ соблюдать Java `equals/hashCode` contract на всём
указанном lifetime. Оба метода ДОЛЖНЫ быть deterministic, side-effect-free,
non-reentrant относительно scheduler, не вызывать никакие scheduler operations
и не бросать exceptions. Это caller precondition, а не проверяемый operational
outcome: после помещения object в hash-based index библиотека не может надёжно
обнаружить нарушение. При нарушении этого precondition identity/linearizability
guarantees не применимы; implementation НЕ ДОЛЖНА заявлять, что способна
атомарно отвергнуть mutable или throwing key после его использования.

Fairness определяется относительно `cost`, а не неизвестного actual execution
time.

### 2.2 Неизменяемая конфигурация instance

**Проектное configuration решение.**

- `D` — число issue slots, целое `1..1_000_000`.
- `maxFlows` — предел одновременно registered flows, целое
  `1..Integer.MAX_VALUE`.
- `maxLiveJobs` — явный предел `queued + dispatched`, целое
  `D..Integer.MAX_VALUE`.
- `cancellationAccounting` — фиксированная policy
  `CancellationAccounting.CHARGE_RESERVED_COST`; альтернативная policy
  отсутствует.

После создания instance все четыре значения неизменяемы. Null, выход за диапазон
и `maxLiveJobs < D` отвергаются до создания observable instance.

Верхняя граница `1_000_000` задаёт допустимое представление и валидацию
конфигурации. Она не утверждает, что атомарный batch такого размера имеет
приемлемые latency, throughput, allocation rate или время удержания внутренней
сериализации. Репозиторная измерительная матрица ограничена `D <= 1_024` и сама
по себе, без сохранённого и проверенного запуска, не является результатом
производительности.

### 2.3 Отображение D на N ресурсов

В Jin04 `D` означает максимум dispatched-but-not-completed requests, а не
число физических ресурсов; см.
[Why the `(D)` matters](THEORY.md#why-the-d-matters).

**Проектное решение.** Scheduler не конфигурирует `N` отдельно. Для модели из
`N` одинаковых parallel non-preemptive ресурсов, где возвращённый job сразу
занимает один ресурс, caller ДОЛЖЕН установить `D = N` и вызывать
`capacityAvailable(k)` только когда способен принять до `k` jobs.

Допускается `D != N` для black-box service с собственной очередью или
ограничением admission depth, но тогда:

- fairness bound использует именно настроенный `D`;
- библиотека гарантирует work conservation issue slots, не физического
  устройства;
- физическая утилизация и момент фактического начала job находятся вне
  контракта.

Аргумент `k` в `capacityAvailable(k)` — максимум результатов данного вызова,
а не сохраняемый permit и не изменение `D`.

## 3. Точные числа и пределы

### 3.1 Семантическое представление

Tag — точное неотрицательное рациональное число `n/d`, где:

- `n` — неотрицательный `BigInteger`;
- `d` — положительный `BigInteger`;
- `gcd(n,d)=1`;
- zero канонически представлен как `0/1`.

Сложение, `max` и сравнение выполняются математически точно. Сравнение `a/b`
и `c/d` использует знак `a*d - c*b`; floating point, decimal rounding и
приближённое деление запрещены. Временные произведения также вычисляются без
fixed-width overflow.

Для принятого job normalized increment равен точной дроби

```text
increment = cost / weight.
```

### 3.2 Numeric budget

**Проектное решение.** После канонического сокращения числитель и знаменатель
каждого сохраняемого tag ДОЛЖНЫ иметь `bitLength <= 4096`. Это часть
fail-closed engineering budget, а не approximation и не обещание принять все
математически корректные traces из syntactically valid `long` inputs.
`NUMERIC_LIMIT` МОЖЕТ возникнуть при допустимых `cost` и `weight`, если exact
history уже создала слишком сложную дробь. Это явная граница production
representation; математический unbounded-rational oracle её не имеет.

Persistent budget относится к `V`, `S/F` queued jobs и `lastFinish` каждого
registered flow. Exact primitive над двумя persistent values МОЖЕТ временно
создавать числитель/знаменатель или cross-product до 8193 bits; это
`MAX_TRANSIENT_BITS`.

Transient budget определяется математическими quantities, а не внутренними
`BigInteger` objects конкретного алгоритма. Для reduced `a/b` и `c/d`
канонические quantities primitive:

```text
add raw numerator       = a*d + c*b
subtract raw numerator  = abs(a*d - c*b)
raw denominator         = b*d
comparison products     = a*d и c*b
rebase subtraction      = те же subtract numerator и denominator
reduced result           = raw numerator/raw denominator после gcd reduction
```

Bit length каждого перечисленного raw/reduced quantity ДОЛЖЕН быть не больше
8193 во время primitive; persistent result после reduction дополнительно
ДОЛЖЕН уложиться в 4096. Implementation МОЖЕТ применять gcd-before-multiply,
cross-cancellation или иной exact algorithm, но принимает/отвергает операцию
так, как если бы проверялись эти канонические mathematical quantities. Размер
случайного implementation temporary object не является API outcome и не может
сам по себе вызвать `NUMERIC_LIMIT`.

Canonical trigger: только если первоначально рассчитанный новый `S` или `F`
не помещается в persistent budget, `enqueue` ДОЛЖЕН ровно один раз рассчитать
точный rebase из §3.3 на временной копии и повторить расчёт. Если rebase или
повторный расчёт не помещается в transient budget, либо хотя бы одно итоговое
сохраняемое значение не помещается в persistent budget, возвращается
`NUMERIC_LIMIT`, а весь observable state остаётся прежним. Иначе rebase и
enqueue фиксируются одной транзакцией. Silent overflow, округление, partial
rebase и изменение order запрещены.

При `V=0` и `lastFinish=0` зарегистрированный flow ДОЛЖЕН принять один job с
любой парой `cost,weight` из `1..Long.MAX_VALUE`, включая максимальные значения,
если не сработал независимый identity/live limit: сокращённая одиночная дробь
занимает не более 63 bits в каждом компоненте.

Число успешно принятых jobs за lifetime instance ограничено
`Long.MAX_VALUE`. State хранит `lastJobSequence` в диапазоне
`0..Long.MAX_VALUE`; новый handle получает `lastJobSequence + 1` только если
`lastJobSequence < Long.MAX_VALUE`. После выдачи sequence `Long.MAX_VALUE`
последующие enqueue возвращают `SEQUENCE_EXHAUSTED`; сложение с переполнением
никогда не выполняется, sequence никогда не переиспользуется.
Неуспешные/no-op вызовы не расходуют sequence.

Flow registrations используют независимый `lastFlowSequence` с теми же
правилами `0..Long.MAX_VALUE`. После исчерпания `registerFlow` возвращает
`FLOW_SEQUENCE_EXHAUSTED`; закрытый FlowHandle никогда не переиспользуется.

Сводные cardinality/lifetime ranges:

```text
running jobs       0..D
queued jobs        0..maxLiveJobs
all live jobs      0..maxLiveJobs
active flows       0..min(maxLiveJobs,maxFlows)
registered flows   0..maxFlows
accepted jobs      0..Long.MAX_VALUE per scheduler instance
flow registrations 0..Long.MAX_VALUE per scheduler instance
stored tag bits    0..4096 per numerator or denominator
transient tag bits 0..8193 per exact primitive component
```

Число read-only и неуспешных вызовов не ограничено scheduler state. Число
успешных cancel/dispatch/complete ограничено числом принятых incarnations.

### 3.3 Exact rebasing

Rebase — представительная замена одного и того же семантического состояния.
Пусть до rebase `B = V`. Тогда атомарно:

1. для каждого queued job: `S := S-B`, `F := F-B`;
2. для каждого registered flow, включая inactive:
   `lastFinish := max(0, lastFinish-B)`;
3. `V := 0`.

Инвариант `S >= V` для queued jobs гарантирует неотрицательность. Tags уже
dispatched jobs scheduler не хранит, поэтому их преобразование не требуется.
Преобразование сохраняет все будущие `max`, increments, сравнения и tie order.

Rebase НЕ МОЖЕТ быть частичным. Он выполняется только по canonical trigger
§3.2 и внутри той же atomic state
transition, что и вызвавшая его операция. Его worst-case time и temporary
space — `O(queuedJobs + registeredFlows)` exact rational components. Если
хотя бы одно преобразованное значение нарушает persistent/transient budget,
временная копия отбрасывается. Proactive, partial или повторный rebase запрещён,
поскольку timing normalization не должен менять наблюдаемый момент
`NUMERIC_LIMIT` между реализациями.

### 3.4 Новый busy period

**Проектное решение на основании допустимой нормализации Goyal96**, описанной
в [Engineering decisions](THEORY.md#engineering-decisions-in-this-library).
Когда после `cancel` или `complete` множество live jobs
становится пустым, в той же atomic transition:

- `V := 0`;
- `lastFinish := 0` для каждого registered flow;
- registration records, numeric sequences и cumulative counters НЕ
  сбрасываются.

Это устраняет перенос tag debt между busy periods и является выбранным
разрешением plain SFQ(D), описанным в
[Why the `(D)` matters](THEORY.md#why-the-d-matters).

## 4. Абстрактное состояние

Scheduler state — кортеж:

```text
Config            = (D, maxFlows, maxLiveJobs)
ownerToken        = inert identity token этого instance
V                 = virtual-time tag
lastJobSequence   = последний выданный job long sequence, изначально 0
lastFlowSequence  = последний выданный flow long sequence, изначально 0
RegisteredById    = FlowId -> FlowHandle
RegisteredFlows   = FlowHandle -> FlowState
LiveById          = JobId -> JobHandle
Queued            = JobHandle -> QueuedJob
Running           = JobHandle -> RunningJob
Priority          = total order queued jobs by (S, jobSequence)
Counters          = accepted, dispatched, cancelled, completed
```

Initial state: оба sequence и все counters равны zero, `V=0`, все maps/sets
пусты, owner token создан; configuration уже провалидирована.

`FlowHandle` и `JobHandle` — нормативно **инертные capabilities**. Каждый
содержит только малый `ownerToken` и соответствующий непереиспользуемый
`long sequence`. Handle НЕ МОЖЕТ содержать reference/backreference на
scheduler, `FlowId`, `JobId`, payload, map, record либо иной caller domain
object. Сам `ownerToken` — отдельный immutable identity marker без полей,
ссылающихся на scheduler или его state. Scheduler и его handles могут
ссылаться на marker, но marker ни на что из них не ссылается.

Handle другого instance отличается owner token. Закрытый/terminal handle
остаётся безопасным inert value, но больше не разрешается ни в один live
record. Returned `Dispatch` содержит payload/IDs для caller; это
caller-owned result, а не содержимое handle или сохраняемый terminal record.

Normative equality обоих handle types:

- два handles равны только если имеют один и тот же runtime type, их
  `ownerToken` — тот же object по identity (`==`) и sequence равен;
- `hashCode` стабилен и выводится только из identity token и sequence;
- `FlowHandle` никогда не равен `JobHandle`, даже при одинаковом числовом
  sequence;
- handle types НЕ реализуют `Comparable` или `Serializable`;
- public API НЕ раскрывает token или sequence accessors.

Таким образом callers могут безопасно использовать handle как opaque map key,
но не могут выводить global order, переносить capability между process/JVM или
конструировать его из числового ID.

```text
FlowState = (flowHandle, flowId, weight, lastFinish,
             queuedCount, runningCount)

QueuedJob = (jobHandle, jobId, flowHandle, payload,
             cost, S, F, jobSequence)

RunningJob = (jobHandle, jobId, flowHandle, cost)
```

Production implementation МОЖЕТ хранить эквивалентное состояние другими
структурами. Reference model ДОЛЖНА предпочесть прямое представление, а не
оптимизацию.

### 4.1 Определения состояния flow

- flow `active`, если `queuedCount + runningCount > 0`;
- flow `backlogged`, если `queuedCount > 0`;
- flow `inactive`, если он registered и оба count равны zero;
- active non-backlogged flow имеет только running jobs.

Registration и activity ортогональны. Flow state и `lastFinish` сохраняются при
active → inactive внутри непустого busy period. Вес неизменяем весь registration
lifetime, включая inactive intervals. Смена веса разрешена только как
успешный debt-safe `closeFlow`, затем новая `registerFlow` с новым FlowHandle.

### 4.2 Инварианты

В каждом observable state:

1. `Queued` и `Running` не пересекаются по handle.
2. Каждый handle находится не более чем в одном lifecycle state.
3. `LiveById` биективно соответствует `Queued union Running` по `JobId`.
4. `RegisteredById` и `RegisteredFlows` биективны по `FlowId/FlowHandle`;
   `|RegisteredFlows| <= maxFlows`.
5. Каждый queued/running job ссылается ровно на один registered flow.
6. `|Queued| + |Running| <= maxLiveJobs`.
7. `|Running| <= D`; `freeSlots = D - |Running|`.
8. Flow counters равны числу соответствующих records; active/backlogged
   являются только производными от counters.
9. Для каждого queued job `S >= V`.
10. `Priority` содержит ровно `Queued` и отсортирован по §6.
11. `V`, queued tags и `lastFinish` всех registered flows каноничны, точны и
    находятся в persistent numeric budget.
12. Payload хранится только в `Queued`, но не в `Running`, handle или terminal
    state.
13. Cumulative lifecycle conservation выполняется математически точно:
    `accepted = |Queued| + |Running| + cancelled + completed` и
    `dispatched = |Running| + completed`.
14. Ни один counter не превышает `Long.MAX_VALUE`; проверки равенств/сумм НЕ
    выполняются с overflowing fixed-width arithmetic.

## 5. Lifecycles и identity

Lifecycle регистрации flow:

```text
ABSENT --registerFlow/REGISTERED--> REGISTERED_INACTIVE
REGISTERED_INACTIVE --enqueue-----> REGISTERED_ACTIVE
REGISTERED_ACTIVE --last terminal-> REGISTERED_INACTIVE
REGISTERED_INACTIVE --closeFlow---> ABSENT
```

`closeFlow` разрешён только для inactive flow с `lastFinish <= V`. При этом
сохранённая identity и новая registration дали бы следующему job один и тот же
start tag `V`, поэтому удаление не стирает действующий fairness debt. Условие
может выполниться как после global idle reset, так и внутри busy period.

Lifecycle одного JobHandle:

```text
ABSENT --enqueue/ACCEPTED--> QUEUED
QUEUED --dispatch----------> RUNNING
QUEUED --cancel/CANCELLED--> ABSENT
RUNNING --complete---------> ABSENT
```

Других переходов нет. Dispatch и cancel необратимы. Running job
non-preemptive с точки зрения scheduler; `cancel` его не отзывает.

### 5.1 Bounded duplicate semantics

**Проектное решение.** Terminal tombstones не хранятся. Поэтому после удаления
handle библиотека намеренно не различает:

- никогда не существовавший handle;
- уже cancelled handle;
- уже completed handle;
- повторный вызов terminal operation.

Все они дают `NOT_LIVE`. Это не временный cache и результат не зависит от
давности вызова.

Поэтому один поздний `cancel/NOT_LIVE` намеренно НЕ сообщает terminal cause:
он не отличает completed-after-dispatch job от ранее cancelled, stale, foreign
или никогда не существовавшего handle. Победитель cancel-versus-dispatch
определяется combined linearized history: наличием handle в dispatch result и
результатом cancel. Пока selected job остаётся running, немедленный cancel даёт
`TOO_LATE_ALREADY_DISPATCHED`; после completion эта диагностическая информация
удалена вместе с bounded terminal metadata.

Caller `JobId` уникален только среди live jobs. Пока ID live, повторный
`enqueue` даёт `DUPLICATE_LIVE_ID`. После terminal transition тот же ID может
быть принят как новая incarnation и получит новый handle. Все lifecycle
operations принимают handle, а не только `JobId`; поэтому запоздалый вызов со
старым handle не может затронуть новую incarnation (нет ABA).

Следовательно, повтор `enqueue` после terminal transition НЕ является
идемпотентным retry: это новый job. Если caller требует бессрочную
идемпотентность business request, он хранит её состояние вне scheduler.

Такая семантика обеспечивает at-most-once без unbounded completed/cancelled
metadata. API НЕ ДОЛЖЕН обещать отдельный `ALREADY_COMPLETED` или
`ALREADY_CANCELLED`, поскольку без retention доказать его невозможно.

Caller `FlowId` уникален среди registered flows. Пока registration существует,
повторный `registerFlow` даёт `DUPLICATE_REGISTERED_ID`. После безопасного
`closeFlow` тот же ID может получить новую registration и новый FlowHandle.
Stale/foreign FlowHandle даёт `FLOW_NOT_REGISTERED` и не может адресовать новую
registration того же ID.

## 6. Tags, virtual time и total order

### 6.1 Enqueue tags

Для принятого job `j` flow `f`:

```text
previousFinish = RegisteredFlows[f].lastFinish
S(j) = max(V, previousFinish)
F(j) = S(j) + cost(j) / RegisteredFlows[f].weight
```

После принятия `RegisteredFlows[f].lastFinish := F(j)`. Enqueue не создаёт
registration и не принимает weight. Для dormant registered flow используется
сохранённый `lastFinish`, а не zero; поэтому inactive interval внутри busy
period не сбрасывает fairness history.

Tags фиксируются при enqueue. Позднейшая cancellation другого queued job НЕ
пересчитывает tags оставшихся jobs и не откатывает `lastFinish`.

Это последнее правило — **проектное cancellation решение**, отсутствующее в
Jin04. Оно предотвращает ретроактивное изменение уже принятых scheduling
decisions. Цена: cancelled supplied cost остаётся virtual charge до конца
текущего глобального busy period; опубликованный completed-work fairness bound
не заявляется для интервалов с cancellation.

### 6.2 Deterministic tie-breaking

**Проектное решение.** Priority key queued job:

```text
(S ascending, jobSequence ascending)
```

`jobSequence` присваивается в linearization order успешных enqueue и уникален.
Это полный детерминированный порядок. Jin04 разрешает arbitrary ties; данный
порядок выбирает один из разрешённых вариантов.

### 6.3 Dispatch и virtual time

Каждый выбранный job — минимальный в текущем `Priority`. Непосредственно перед
его переходом в running:

```text
V := S(selected)
```

При batch dispatch jobs выбираются последовательно; обновлённое `V` относится
к следующему выбору. Поскольку queued order не убывает по `S`, `V` не
убывает внутри busy period и может остаться равным прежнему.

`complete` само по себе не продвигает `V`; оно лишь освобождает issue slot.
`V` меняется при dispatch, exact rebase или reset на границе busy period.

## 7. Операции и результаты

Все проверки, расчёты и изменения одной операции логически атомарны.
Invalid arguments отвергаются до state mutation. Конкретный Java API МОЖЕТ
представить programmer errors исключением, а перечисленные operational
outcomes — value result, но это различие ДОЛЖНО быть единообразно
документировано в JavaDoc.

### 7.1 `registerFlow(flowId, weight)`

Порядок:

1. Проверить non-null `flowId` и `weight` в `1..Long.MAX_VALUE`.
2. Если `flowId` есть в `RegisteredById`, вернуть
   `DUPLICATE_REGISTERED_ID`.
3. Если registered count равен `maxFlows`, вернуть `FLOW_LIMIT`.
4. Если `lastFlowSequence == Long.MAX_VALUE`, вернуть
   `FLOW_SEQUENCE_EXHAUSTED`.
5. Создать inert `FlowHandle(ownerToken,lastFlowSequence+1)` и FlowState с
   `lastFinish=0`, zero counts и fixed weight; вставить оба registration
   indexes, обновить sequence.
6. Вернуть `REGISTERED(flowHandle)`.

Registration во время непустого busy period разрешена: до первого enqueue она
не влияет на scheduling. Любой rejection не меняет state и не расходует
sequence.

### 7.2 `closeFlow(flowHandle)`

Null handle — invalid argument. Для foreign, stale или уже закрытого handle
вернуть `FLOW_NOT_REGISTERED`.

- Если registered flow active, вернуть `FLOW_ACTIVE`.
- Если flow inactive, но `lastFinish > V`, вернуть
  `FAIRNESS_DEBT_ACTIVE`.
- Если flow inactive и `lastFinish <= V`, атомарно удалить его из
  `RegisteredFlows` и `RegisteredById`, освободить internal `FlowId` reference
  и вернуть `CLOSED`.

Условие закрытия fairness-neutral, поскольку при сохранении старой identity
следующий enqueue получил бы `S=max(V,lastFinish)=V`, а новая registration с
`lastFinish=0` также получит `S=V`. Global idle reset §3.4 является достаточным,
но не обязательным способом достичь этого условия. Успех разрешает повторную
регистрацию того же `FlowId`, но новый FlowHandle получает новый sequence.

### 7.3 `enqueue(flowHandle, jobId, payload, cost)`

Порядок:

1. Проверить non-null handle/ID/payload и `cost` range.
2. Если FlowHandle foreign, stale или closed, вернуть `FLOW_NOT_REGISTERED`.
3. Если `jobId` есть в `LiveById`, вернуть `DUPLICATE_LIVE_ID`.
4. Если live count равен `maxLiveJobs`, вернуть `LIVE_LIMIT`.
5. Если job sequence исчерпан, вернуть `SEQUENCE_EXHAUSTED`.
6. Используя fixed registered weight/lastFinish, вычислить точные `S`, `F`; при
   необходимости транзакционно применить §3.3 ко всей требуемой state copy.
   Если budget всё равно нарушен, вернуть `NUMERIC_LIMIT`.
7. Создать inert JobHandle/queued record, вставить его во все job indexes,
   обновить registered flow counts/lastFinish, counters и job sequence.
8. Вернуть `ACCEPTED(jobHandle)`.

Любой результат кроме `ACCEPTED` не меняет observable state и не расходует
sequence.

### 7.4 `cancel(handle)`

Null handle — invalid argument. Opaque handle другого scheduler instance
трактуется как `NOT_LIVE`.

- Если handle в `Queued`, атомарно удалить job из очереди, priority и
  `LiveById`, уменьшить flow count, увеличить `cancelled`, освободить payload и
  вернуть `CANCELLED`.
- Если handle в `Running`, ничего не менять и вернуть
  `TOO_LATE_ALREADY_DISPATCHED`.
- Иначе вернуть `NOT_LIVE`.

JavaDoc ДОЛЖЕН явно говорить, что `NOT_LIVE` подтверждает только отсутствие
live job в LP и сам по себе не доказывает, какая terminal operation произошла.
Caller сопоставляет его с ранее полученными dispatch/cancel/completion results,
если ему нужна причина.

Registered flow state при deactivation сохраняется. Если удалён последний live
job scheduler, выполняется reset §3.4 для всех registrations в той же
transition. Cancellation не возвращает capacity: queued job её не занимал.

### 7.5 `capacityAvailable(k)` / `dispatch(k)`

`k` — целое `0..D`; отрицательное или `k>D` — invalid argument. `k=0`
возвращает пустой список без mutation.

Число выбираемых jobs:

```text
m = min(k, D - |Running|, |Queued|).
```

Для `i=1..m` операция последовательно:

1. берёт minimum `Priority`;
2. устанавливает `V := S(job)`;
3. удаляет queued record и payload из внутренних queued structures;
4. создаёт `RunningJob`, сохраняя handle, IDs, flow и cost, но не payload;
5. обновляет flow counts и `dispatched`;
6. добавляет `Dispatch(handle, jobId, flowId, payload, cost)` в result list.

Result list упорядочен фактическим dispatch order. Весь batch — одна atomic
operation: другой вызов не может вклиниться между его элементами. Если `m=0`,
возвращается пустой список.

Каждый `Dispatch` — immutable detached carrier полей
`(jobHandle, jobId, flowId, payload, cost)`, но намеренно сохраняет обычную
Object identity equality/hashCode и НЕ является value record: для payload не
задан `equals/hashCode` precondition, поэтому structural equality создала бы
ложный API contract. Returned list также immutable/unmodifiable и detached от
внутренних collections; scheduler никогда не изменяет ни carrier, ни список
после возврата.

Differential comparison сравнивает поля carrier явно: соответствующие opaque
handles через logical mapping, IDs по их contract, `cost` численно, а payload
только по object identity (`==`) для одного и того же input trace. `Dispatch`
objects или result lists не сравниваются через value `equals`.

Каждый результат немедленно и необратимо расходует один issue slot до
успешного `complete`. Повторный `capacityAvailable` не может выдать тот же slot
или job. Caller ДОЛЖЕН вызывать операцию только будучи готовым принять весь
возвращаемый batch. Сбой caller/executor после возврата не откатывает dispatch;
caller всё равно обязан завершить handle через `complete`. Requeue — новая
incarnation через новый enqueue и не входит в эту операцию.

### 7.6 `complete(handle)`

Null handle — invalid argument. Opaque handle другого scheduler instance
трактуется как `NOT_LIVE`.

- Если handle в `Running`, атомарно удалить running record и `LiveById`,
  уменьшить flow count, увеличить `completed`, освободить один internal issue
  slot и вернуть `COMPLETED`.
- Если handle в `Queued`, ничего не менять и вернуть `NOT_DISPATCHED`.
- Иначе вернуть `NOT_LIVE`.

Registered flow state при deactivation сохраняется. При удалении последнего
live job выполняется reset §3.4 для всех registrations. Completion НЕ
dispatch-ит следующий job автоматически; caller вызывает `capacityAvailable`
отдельно.

### 7.7 `snapshot()`

Snapshot содержит как минимум:

```text
D, maxFlows, maxLiveJobs,
registeredFlows,
queuedJobs, runningJobs, freeSlots,
activeFlows, backloggedFlows,
acceptedTotal, dispatchedTotal, cancelledTotal, completedTotal
```

Snapshot не содержит payload, identifiers, handles или internal tags. Он —
точный immutable atomic snapshot одного linearization point, а не weakly
consistent iteration. Cumulative counters не включают failed/no-op outcomes.

### 7.8 `snapshot(flowHandle)`

Null handle — invalid argument. Для exact capability текущей регистрации
операция возвращает immutable `FlowSnapshot`:

```text
queuedJobs, runningJobs,
acceptedCost, dispatchedCost, cancelledCost
```

Job counts относятся к текущему состоянию. Cost totals — точные неотрицательные
целые суммы supplied cost за lifetime этой регистрации:

- `acceptedCost` увеличивается только успешным enqueue;
- `dispatchedCost` увеличивается для каждого job в успешном dispatch batch;
- `cancelledCost` увеличивается только успешным queued cancel;
- текущий `queuedCost = acceptedCost - dispatchedCost - cancelledCost`.

Completion не меняет cost totals: `dispatchedCost` включает running и completed
jobs. Failed/no-op outcomes не меняют snapshot. Cost totals НЕ переполняются:
они представлены exact integers и ограничены глобальным never-reused job
sequence, поэтому каждое значение не превышает
`Long.MAX_VALUE * Long.MAX_VALUE`.

Для foreign, stale либо closed capability операция возвращает empty. Snapshot
не содержит FlowId, handle, payload, weight, internal tags или clock-derived
age. Scheduler не владеет clock и не вызывает пользовательские metrics
callbacks. Текущий weight известен caller из успешной регистрации; enqueue
timestamp/oldest age при необходимости остаётся в caller payload или внешнем
observer.

## 8. Linearization points и races

Public operations полностью thread-safe и линейризуемы. Конкретный lock/CAS
не является контрактом; linearization point (LP) — абстрактный момент atomic
commit/observation:

| Операция и результат | Linearization point |
|---|---|
| `registerFlow/REGISTERED` | atomic commit обоих registration indexes и flow sequence |
| `registerFlow/*rejection*` | atomic observation первой применимой registration проверки; state не меняется |
| `closeFlow/CLOSED` | atomic removal из `RegisteredFlows` и `RegisteredById` |
| `closeFlow/FLOW_ACTIVE` | atomic observation ненулевого flow job count |
| `closeFlow/FAIRNESS_DEBT_ACTIVE` | atomic observation inactive flow с `lastFinish > V` |
| `closeFlow/FLOW_NOT_REGISTERED` | atomic observation отсутствия exact capability в registry |
| `enqueue/ACCEPTED` | atomic commit вставки job, flow/tag updates, sequence и counter |
| `enqueue/*rejection*` | atomic observation, на котором выполнена первая применимая проверка результата; state не меняется |
| `cancel/CANCELLED` | atomic removal из `Queued` и `LiveById`, включая flow/reset/counter updates |
| `cancel/TOO_LATE_ALREADY_DISPATCHED` | atomic observation handle в `Running` |
| `cancel/NOT_LIVE` | atomic observation отсутствия handle в `Queued` и `Running` |
| `dispatch/non-empty` | единый atomic commit всех `m` transitions и последнего значения `V` |
| `dispatch/empty` | atomic observation `k=0` либо отсутствия одновременно slot и/или queued job |
| `complete/COMPLETED` | atomic removal из `Running` и `LiveById`, включая slot/flow/reset/counter updates |
| `complete/NOT_DISPATCHED` | atomic observation handle в `Queued` |
| `complete/NOT_LIVE` | atomic observation отсутствия handle в `Queued` и `Running` |
| `snapshot` | atomic capture всех перечисленных полей |
| `snapshot(flowHandle)` | atomic lookup регистрации и capture всех flow fields либо observation её отсутствия |

Rebase, busy-period reset и payload release являются частью LP вызвавшей их
операции, а не отдельными public events.

### 8.1 Обязательные race outcomes

- `cancel` против batch `dispatch`: если cancel LP раньше batch LP, cancel
  возвращает `CANCELLED`, а handle отсутствует в dispatch result. Если batch LP
  раньше **и этот handle выбран**, handle возвращён ровно один раз, а cancel
  даёт `TOO_LATE_ALREADY_DISPATCHED` либо поздний `NOT_LIVE` после completion.
  Если более ранний batch выбрал только другие jobs и оставил этот handle в
  `Queued`, последующий cancel МОЖЕТ вернуть `CANCELLED`. Сам факт более раннего
  dispatch LP не блокирует cancellation невыбранного job.
- Два `dispatch`: batches полностью упорядочены по LP; capacity и jobs между
  ними не дублируются.
- Два `complete`: ровно один может вернуть `COMPLETED`; остальные возвращают
  `NOT_LIVE`.
- Два `cancel`: ровно один может вернуть `CANCELLED`; остальные возвращают
  `NOT_LIVE`.
- `complete` против `cancel` queued job: cancel может вернуть `CANCELLED`, а
  completion — `NOT_DISPATCHED` до него или `NOT_LIVE` после него. Completion
  queued job успешным быть не может.
- `enqueue` одинакового live `JobId`: ровно один может получить `ACCEPTED`;
  остальные видят `DUPLICATE_LIVE_ID`, пока первая incarnation live.
- `completion + dispatch`: если completion LP раньше, освободившийся slot
  доступен batch; иначе dispatch его не использует и caller должен повторить
  вызов.
- `closeFlow + enqueue` одного FlowHandle: если `enqueue/ACCEPTED` LP раньше,
  close видит active flow и возвращает `FLOW_ACTIVE`; если `closeFlow/CLOSED`
  LP раньше, enqueue возвращает `FLOW_NOT_REGISTERED`. Rejected enqueue
  (`DUPLICATE_LIVE_ID`, `LIVE_LIMIT`, `SEQUENCE_EXHAUSTED`, `NUMERIC_LIMIT` или
  иной rejection) — atomic no-op: если его LP раньше close, close вычисляет
  `CLOSED`, `FAIRNESS_DEBT_ACTIVE` или `FLOW_ACTIVE` только по неизменённому
  предшествующему state. Job не может ссылаться на удалённый flow.
- `closeFlow` inactive flow против последнего completion/cancel другого flow:
  если до terminal LP `lastFinish > V`, close возвращает
  `FAIRNESS_DEBT_ACTIVE`; terminal LP сначала выполняет global reset, после
  чего close может вернуть `CLOSED`. Если debt уже погашен, close может вернуть
  `CLOSED` и до terminal LP.
- Concurrent registrations при `maxFlows` не могут вместе превысить limit;
  LP order даёт лишнему вызову `FLOW_LIMIT`. Register того же `FlowId` против
  close старой registration даёт либо `DUPLICATE_REGISTERED_ID`, либо новый
  distinct FlowHandle после `CLOSED`.
- `closeFlow` против enqueue, требующего rebase: rebase существует только как
  часть `enqueue/ACCEPTED` LP. Enqueue/rebase-first активирует flow, после чего
  close возвращает `FLOW_ACTIVE`. Close-first возвращает `CLOSED`, если
  `lastFinish <= V`, и последующий enqueue даёт `FLOW_NOT_REGISTERED`; при
  `lastFinish > V` close возвращает `FAIRNESS_DEBT_ACTIVE`, сохраняет
  registration, и enqueue всё ещё может атомарно выполнить rebase.
  `enqueue/NUMERIC_LIMIT` отбрасывает всю temporary copy, поэтому последующий
  close решается по неизменённому сравнению `lastFinish` с `V`. Rebase другого
  flow преобразует оба значения общей нормализацией §3.3 и сохраняет истинность
  debt-safe условия.

Эти правила обеспечивают history, эквивалентную некоторому допустимому
последовательному execution. Cancel-versus-dispatch winner восстанавливается
по combined results/history: `CANCELLED` плюс отсутствие handle в batch
означает победу cancel; presence handle в batch означает победу dispatch.
Один поздний `NOT_LIVE` без истории намеренно не раскрывает причину отсутствия.
Public JavaDoc для cancel и dispatch ДОЛЖЕН описать именно этот combined
winner contract и не обещать cause из одного `NOT_LIVE`.

## 9. Activation, deactivation и cancellation charge

### 9.1 Inactive → active

Успешный enqueue registered inactive flow использует
`S=max(V,lastFinish)`. Registration уже существует; activity меняется только
из-за job count. Внутри непустого busy period dormant flow сохраняет свой
finish history и не получает возможность сбросить его через краткую
неактивность. Если scheduler был полностью idle, reset §3.4 уже установил
`V=0` и все registered `lastFinish=0`, то есть начинается новый normalized
busy period.

### 9.2 Backlogged → active non-backlogged

Dispatch последнего queued job flow делает его non-backlogged, если у него
остаётся хотя бы один running job. State, weight и `lastFinish` сохраняются.
Новый enqueue до последнего completion использует этот `lastFinish`.

### 9.3 Active → inactive

После cancel/complete, уменьшившего оба flow counters до zero, registration,
weight и `lastFinish` сохраняются. Последующий enqueue того же FlowHandle в
этом busy period использует `max(V,lastFinish)`. Пока `lastFinish > V`, сменить
weight или получить новую fairness identity нельзя. Когда `V` достигает
`lastFinish`, registration можно безопасно закрыть, не меняя start tag
следующего возможного job.

Virtual charge cancelled job сохраняется даже после deactivation до global
idle reset. Это намеренная non-retroactive semantics §6.1.

### 9.4 Registered → closed

Inactive не означает closed. `closeFlow` удаляет identity только при
`lastFinish <= V`. Поэтому close+register, в том числе с другим weight, не
уменьшает start tag следующего job: до и после операции он равен `V`.

## 10. Гарантии и границы claims

Для traces без cancellation ядро соответствует plain SFQ(D) Jin04 §3.2:

- `S=max(V,F_previous)` и `F=S+cost/weight`;
- dispatch в неубывающем start-tag order;
- `V` равен start tag последнего dispatch;
- одновременно running не более `D`;
- `D=1` даёт SFQ при том же tie rule; flow registration нельзя заменить, пока
  её `lastFinish > V`;
- при queued work, положительном `k` и свободном slot dispatch возвращает job;
- один backlogged flow может занять все `D` slots.

Опубликованный pairwise completed-work bound из
[What the papers support](THEORY.md#what-the-papers-support) применим
только при его предпосылках, включая непрерывный backlog обоих flows,
положительные фиксированные weights, конечные per-flow maximum costs и
publication-compatible trace:

```text
|W_f/weight_f - W_g/weight_g|
<= (D+1) * (c_f_max/weight_f + c_g_max/weight_g).
```

Units — supplied cost. Для интервалов с cancellation документ не заявляет этот
bound, потому что non-retroactive virtual charge не является completed work.

No-starvation заявляется только при предпосылках
[Starvation and progress](THEORY.md#starvation-and-progress): bounded
registry/очередь, положительные fixed-for-registration weights, положительная
нижняя граница normalized increment, данный FIFO tie rule, конечное завершение
каждого dispatched job и продолжающиеся completion/dispatch calls. В этой
спецификации `maxFlows/maxLiveJobs` дают конечность, а входные ranges дают
`cost/weight >= 1/Long.MAX_VALUE`. Close/new identity внутри busy period
разрешены только после погашения debt, когда reset identity не уменьшает
следующий start tag. Без внешнего progress scheduler не может гарантировать
dispatch.

Обязательный adversarial trace: accepted head victim остаётся queued с
фиксированным `S_v`; competing registered flow после каждого completion
временно становится inactive и re-enqueue-ится до следующего dispatch. Его
`lastFinish` НЕ может быть сброшен, пока он больше `V`, поэтому start tags
каждого следующего request растут минимум на его положительный normalized
increment. После `lastFinish <= V` новая identity всё равно начинает с `V` и
не получает меньший key. При bounded registry и FIFO ties лишь конечное число
requests может иметь key меньше key victim; victim должен быть dispatch-нут.
Реализация, которая удаляет inactive flow при `lastFinish > V`, этот must-pass
trace не проходит и не соответствует спецификации.

Work conservation означает: каждый вызов `capacityAvailable(k>0)` заполняет
`min(k, freeSlots, queuedJobs)` issue slots. Это не обещание автоматического
callback и не гарантия насыщения physical resource при неверном `D`, отсутствии
вызовов или executor failure.

## 11. Resource retention и bounds

Scheduler хранит `O(liveJobs + registeredFlows)` records. При
`liveJobs <= maxLiveJobs` и `registeredFlows <= maxFlows` cardinality bounded;
никакие структуры не растут с числом terminal jobs или прошлых registrations.

- Успешный cancel удаляет payload, job ID и queued record в своём LP.
- Dispatch удаляет internal payload reference в своём LP; payload остаётся у
  caller только через returned result.
- Completion удаляет job ID и running record в своём LP.
- Deactivation сохраняет только bounded registration state; успешный
  `closeFlow` удаляет flow ID/state.
- Terminal handles/IDs/results не кэшируются.
- Handles содержат только inert owner token/sequence и не удерживают scheduler
  или caller domain objects.
- Единственные scheduler-wide lifetime значения — четыре fixed-width counters
  и два fixed-width sequence, каждый ограничен `Long.MAX_VALUE`.
- Каждая registration хранит три exact cost totals. Never-reused job sequence
  ограничивает каждую сумму значением
  `Long.MAX_VALUE * Long.MAX_VALUE` (не более 126 bits); successful close
  удаляет эти totals вместе с registration state.
- Exact tag components ограничены 4096 bits и exact rebase; numeric limit
  приводит к явному отказу enqueue, а не неограниченному росту. Rebase требует
  `O(queuedJobs + registeredFlows)` bounded temporary state.

`JobHandle`, `FlowHandle`, dispatch result или snapshot, сохранённые caller,
находятся вне internal retention библиотеки; благодаря inert handle они не
удерживают scheduler transitively.

## 12. Deviations и engineering resolutions относительно Jin04

| Тема | Jin04 | Решение проекта и последствие |
|---|---|---|
| Physical `N` | `D` — outstanding issue depth black-box server | Для непосредственных `N` ресурсов требуется `D=N`; иные mappings — external admission model |
| API/capacity | Нет Java API и caller permits | `capacityAvailable(k)` — atomic bounded batch request; completion не вызывает callback |
| Tie | Ties arbitrary | Total key `(S, admission sequence)` |
| Busy-period boundary | Plain §3.2 не даёт полного правила | При global idle `V` и `lastFinish` всех registrations обнуляются, registrations остаются |
| Cancellation | Отсутствует | Только queued cancel; immutable tags, virtual charge сохраняется до global idle; fairness theorem scoped away from cancelled intervals |
| Flow identity | Flow предполагается устойчивой алгоритмической сущностью, API lifecycle отсутствует | Bounded registration, persistent dormant `lastFinish`, close только inactive с `lastFinish <= V` |
| Job identity/duplicates | Отсутствуют | Inert never-reused capability handles, live-only JobId uniqueness, terminal `NOT_LIVE`, no tombstones |
| Concurrency | Отсутствует | Linearizable atomic operations и exact LP table §8 |
| Batch dispatch | Описано заполнение depth, без API atomicity | Один call выбирает последовательный SFQ(D) batch, но linearizes целиком |
| Completion order | Black-box server | Любой running handle может complete; scheduler order не навязывает |
| Weight changes | Не определены | Weight fixed registration lifetime; смена только safe close+new registration |
| Numbers | Математические unbounded tags | Exact canonical rational, fail-closed 4096-bit persistent/8193-bit transient budgets, transactional all-registration rebase |
| Retention | Не рассматривается | Payload release, no terminal metadata, `maxLiveJobs` и `maxFlows` bounds |
| Introspection | Не рассматривается | Exact atomic aggregate snapshot и per-registration lifecycle snapshot без tags/identifiers/clock |
| Executor rejection | Не рассматривается | Dispatch irrevocable; caller обязан complete, requeue является новым enqueue |

Min-SFQ(D), FSFQ(D), FlashFQ, MSFQ, MSF²Q и MQFQ не добавляются. В частности,
нет adjusted tags, GPS eligibility, anticipation, throttling threshold или
multi-queue relaxed order.

## 13. Model-testing obligations

Reference oracle использует unbounded exact rationals и не отвергает
синтаксически допустимый enqueue из-за bit budget. Production implementation
ДОЛЖНА совпадать с oracle по каждому принятому prefix до ожидаемого bounded
rejection. При `NUMERIC_LIMIT` comparison harness ДОЛЖЕН подтвердить на
unbounded candidate, что формальный persistent/transient budget действительно
нарушен после единственной допустимой transactional rebase; production state
не меняется, а oracle state откатывается для продолжения общего trace.

При отсутствии такого expected rejection reference model и production
implementation ДОЛЖНЫ совпадать по:

- register/close outcomes; harness создаёт logical bijection
  `oracle FlowHandle <-> SUT FlowHandle` по соответствующему успешному
  register event, не читая token/sequence;
- enqueue outcomes; аналогичная logical bijection JobHandle создаётся по
  соответствующему `ACCEPTED` event;
- ordered dispatch lists, сравниваемые field-by-field по §7.5 через эти
  logical handle mappings;
- cancel/completion results;
- aggregate и per-flow snapshots;
- rejection без state mutation;
- busy-period reset, persistent dormant histories, snapshot counts и exact
  per-flow lifecycle costs.

Для concurrency history результаты должны допускать хотя бы одну
последовательность по §8. Обязательные model properties:

1. формулы и exact comparison tags;
2. monotonic dispatch start tags внутри busy period;
3. total deterministic ties;
4. `running <= D` и exact slot accounting;
5. at-most-once dispatch/completion/cancel success;
6. membership-sensitive cancel/batch race: cancel-winner никогда не
   dispatch-ится, выбранный batch job не cancel-ится, а невыбранный queued job
   может быть cancelled после более раннего batch;
7. registered/inactive/active transitions, immutable registration weight и
   safe close;
8. JobId reuse без ABA благодаря handle;
9. FlowId reuse без ABA благодаря inert FlowHandle;
10. exact all-registration rebase equivalence, transient/persistent limits и
    transactional numeric rejection against unbounded oracle;
11. adversarial dormant-flow starvation trace из §10;
12. concurrent `maxFlows` capacity, register/close/enqueue/global-idle/rebase
    races из §8.1;
13. bounded record cardinality и отсутствие terminal/payload retention.
14. late `cancel/NOT_LIVE` допускает все terminal/stale/foreign причины и не
    используется как самостоятельное доказательство dispatch winner.
15. conservation equations §4.2 после каждого event, с mathematical sums без
    test-side `long` overflow;
16. exact handle equality/hash contract, разные runtime types, отсутствие
    `Comparable`/`Serializable` и public token/sequence accessors;
17. identity equality immutable `Dispatch`, unmodifiable detached result list
    и field-by-field differential comparison с payload identity.

Любая будущая оптимизация обязана сохранять эту наблюдаемую модель. Изменение
любого решения в §12 требует сначала изменить эту спецификацию, claim scope и
model tests.
