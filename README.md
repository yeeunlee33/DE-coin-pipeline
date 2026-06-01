# 코인 실시간 데이터 파이프라인

업비트 실시간 체결 데이터를 Kafka로 수집하고, Spark Streaming으로 1분봉 OHLCV를 집계해 PostgreSQL에 저장하는 파이프라인입니다.

---

## 아키텍처

```
업비트 WebSocket
      ↓
  Kafka (upbit-trades 토픽)
      ↓
  Spark Streaming (1분봉 집계)
      ↓
  PostgreSQL (ohlcv 테이블)
```

---

## 기술 스택

- **Kafka** : 실시간 체결 데이터 수집 및 버퍼링
- **PySpark** : 스트리밍 데이터 집계 (Structured Streaming)
- **PostgreSQL** : 1분봉 OHLCV 저장
- **Docker** : Kafka, PostgreSQL 컨테이너 실행

---

## 폴더 구조

```
coin-pipeline/
├── ingestion/
│   ├── upbit_ws_producer.py   # 업비트 WebSocket → Kafka 전송
│   ├── kafka_config.py        # Kafka 설정 (브로커 주소, 토픽, 코인 목록)
│   └── requirements.txt
├── processing/
│   ├── spark_streaming_job.py # Spark 스트리밍 메인 실행 파일
│   ├── window_aggregation.py  # 1분봉 OHLCV 집계 로직
│   ├── anomaly_detection.py   # 이상 탐지 (개발 중)
│   ├── schemas.py             # Kafka 메시지 스키마 정의
│   └── requirements.txt
├── db/
│   └── init.sql               # PostgreSQL 테이블 DDL
└── docker-compose.yml
```

---

## 실행 방법

### 1. 컨테이너 실행

```bash
docker compose up -d
```

### 2. 업비트 Producer 실행

```bash
cd ingestion
python upbit_ws_producer.py
```

### 3. Spark Streaming Job 실행

```bash
cd processing
python spark_streaming_job.py
```

### 4. 데이터 확인

```bash
docker exec -it postgres psql -U postgres -d coindb -c "SELECT * FROM ohlcv LIMIT 10;"
```

---

## 트러블슈팅

### 1. Kafka UI topics 무한 로딩

**문제**

Kafka UI는 뜨는데 topics 탭이 무한 로딩됨.

**원인**

`KAFKA_ADVERTISED_LISTENERS`가 `PLAINTEXT://localhost:9092`로 설정되어 있었음. Kafka UI는 도커 컨테이너 안에서 실행되기 때문에 `localhost`가 자기 자신을 가리켜서 Kafka를 못 찾음.

**해결**

도커 내부 컨테이너끼리는 컨테이너 이름으로 통신하고, 외부(PC)에서는 별도 포트를 쓰도록 리스너를 분리함.

```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
```

```
PC (Python Producer)     → localhost:29092
도커 내부 (Kafka UI 등)  → kafka:9092
```

---

### 2. Spark + Kafka 연결 시 Hadoop 오류

**문제**

단순 Spark 테스트는 됐는데 `readStream.format("kafka")` 추가하자마자 오류 발생.

```
HADOOP_HOME and hadoop.home.dir are unset
UnsatisfiedLinkError: NativeIO$Windows.access0
```

**원인**

Spark Streaming은 체크포인트 저장 시 내부적으로 Hadoop 파일시스템 API를 사용함. Windows 환경에서는 `winutils.exe`와 `hadoop.dll`이 없으면 동작하지 않음. 단순 DataFrame 연산은 Hadoop을 거의 안 써서 이전에는 넘어갔던 것.

**해결**

1. `winutils.exe`, `hadoop.dll` 다운로드 후 `C:/hadoop/bin/`에 배치
2. 환경변수 및 코드에 경로 지정

```python
os.environ["HADOOP_HOME"] = "C:/hadoop"
```

---

### 3. Spark-Kafka 버전 불일치

**문제**

```
java.lang.NoSuchMethodError: scala.Predef$.wrapRefArray
```

**원인**

설치된 Spark 버전과 Kafka connector 버전의 Scala 버전이 맞지 않음.

**해결**

Spark 버전을 3.5.1로 낮추고 connector 버전도 맞춰서 재설치.

---

### 4. Consumer group offset 저장 실패

**문제**

`group_id`를 지정한 Consumer는 동작하지 않고, `group_id=None`일 때만 정상 동작함.

**원인**

Kafka는 Consumer group의 offset을 `__consumer_offsets`라는 내부 토픽에 저장함. 이 토픽 생성 시 기본 복제본 수가 3인데, 브로커가 1대뿐인 환경에서는 복제본을 채울 수 없어서 토픽 생성 자체가 실패함. `group_id=None`은 offset을 저장하지 않는 모드라 이 문제를 피해간 것.

**해결**

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
```

복제본 수를 1로 명시해서 단일 브로커에서도 내부 토픽이 생성되도록 함.

---

### 5. PostgreSQL 중복 저장 문제

**문제**

같은 1분 윈도우(예: 21:08:00 ~ 21:09:00)의 KRW-BTC 데이터가 여러 row로 중복 저장됨.

**원인**

Spark Structured Streaming의 `outputMode("update")`는 윈도우가 업데이트될 때마다 결과를 내보냄. 1분이 끝나기 전에도 데이터가 들어올 때마다 중간 집계 결과를 저장하기 때문에 같은 윈도우가 여러 번 저장됨.

```
21:08:10  BTC 체결 → 저장 (open=100, close=100)
21:08:30  BTC 체결 → 또 저장 (open=100, close=105)  ← 중복
21:08:50  BTC 체결 → 또 저장 (open=100, close=98)   ← 중복
```

**해결**

JDBC `append` 방식 → psycopg2 `upsert` 방식으로 교체.

`(code, window_start)` 조합에 UNIQUE 제약조건을 걸고, 같은 조합이 들어오면 INSERT 대신 UPDATE하도록 변경.

```sql
INSERT INTO ohlcv (code, window_start, ...)
VALUES (...)
ON CONFLICT (code, window_start)
DO UPDATE SET
    close = EXCLUDED.close,
    high  = EXCLUDED.high,
    ...
```

이로써 항상 최신 집계값으로 덮어쓰이고 중복이 사라짐.

---

### 6. init.sql 자동 실행 안 됨

**문제**

`docker-compose.yml`에 `init.sql`을 마운트했는데 테이블이 생성되지 않음.

**원인**

PostgreSQL 이미지는 `/docker-entrypoint-initdb.d/` 폴더의 SQL 파일을 **컨테이너 첫 시작 시에만** 실행함. 이미 볼륨에 데이터가 있으면 초기화 과정을 건너뜀.

**해결**

기존 볼륨을 삭제하고 컨테이너를 새로 띄움.

```bash
docker compose down -v
docker compose up -d
```
