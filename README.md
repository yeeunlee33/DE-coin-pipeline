# 코인 실시간 데이터 파이프라인

업비트 실시간 체결 데이터를 Kafka로 수집하고, Spark Streaming으로 1분봉 OHLCV를 집계해 PostgreSQL에 저장하는 파이프라인입니다.
이상 탐지 시 텔레그램 알람을 전송하고, Airflow로 코인별 임계값을 매일 재계산합니다.

---

## 아키텍처

```
업비트 WebSocket
      ↓
  Kafka (upbit-trades 토픽)
      ↓
  Spark Streaming (1분봉 집계 + 이상 탐지)
      ↓
  PostgreSQL
  ├── ohlcv            (1분봉 데이터)
  ├── anomaly_log      (이상 탐지 기록)
  └── anomaly_threshold (Airflow가 매일 재계산한 임계값)
      ↓
  Grafana (시각화) / 텔레그램 (실시간 알람)

  Airflow (매일 자정)
  └── ohlcv 7일치 분석 → 코인별 임계값 재계산 → anomaly_threshold 업데이트
```

---

## 기술 스택

- **Kafka** : 실시간 체결 데이터 수집 및 버퍼링
- **PySpark** : 스트리밍 데이터 집계 (Structured Streaming)
- **PostgreSQL** : 1분봉 OHLCV 저장, 이상 탐지 로그 저장
- **Airflow** : 코인별 이상 탐지 임계값 매일 재계산 (통계 기반)
- **Grafana** : 실시간 분봉 시각화 대시보드
- **Docker** : 전체 인프라 컨테이너 실행
- **텔레그램 Bot** : 이상 탐지 실시간 알람

---

## 폴더 구조

```
coin-pipeline/
├── ingestion/
│   ├── upbit_ws_producer.py   # 업비트 WebSocket → Kafka 전송
│   ├── kafka_config.py        # Kafka 설정 (브로커 주소, 토픽, 코인 목록)
│   └── requirements.txt
├── processing/
│   ├── spark_streaming_job.py # Spark 스트리밍 메인 실행 파일 (이상 탐지 + 텔레그램 포함)
│   ├── window_aggregation.py  # 1분봉 OHLCV 집계 로직
│   ├── anomaly_detection.py   # 이상 탐지 유틸 함수
│   ├── schemas.py             # Kafka 메시지 스키마 정의
│   └── requirements.txt
├── airflow/
│   └── dags/
│       └── threshold_update_dag.py  # 코인별 임계값 매일 재계산 DAG
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

### 4. 텔레그램 봇 설정 (선택)

이상 탐지 알람을 받으려면 `.env` 파일에 설정. 없으면 알람 없이 정상 동작.

```dotenv
telegram_token=your_token
telegram_cht_id=your_chat_id
```

### 5. Airflow DAG 실행

`localhost:8081` 접속 → admin/admin 로그인 → `threshold_update` DAG 수동 실행
매일 자정 자동 실행되므로 이후에는 별도 조작 불필요.

### 6. Grafana 대시보드

`localhost:3000` 접속 → admin/admin 로그인 → PostgreSQL 데이터소스 연결 후 대시보드 생성

### 7. 데이터 확인

```bash
# 분봉 데이터
docker exec -it postgres psql -U postgres -d coindb -c "SELECT * FROM ohlcv LIMIT 10;"
# 이상 탐지 기록
docker exec -it postgres psql -U postgres -d coindb -c "SELECT * FROM anomaly_log LIMIT 10;"
# 코인별 임계값
docker exec -it postgres psql -U postgres -d coindb -c "SELECT * FROM anomaly_threshold;"
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

---

### 7. Streaming에서 lag()로 이상 탐지 불가

**문제**

Spark `Window + lag()`로 직전 분봉 종가를 참조하려 했으나 동작하지 않음.

**원인**

`lag()`는 정적 DataFrame 전용 함수임. Structured Streaming은 데이터가 배치 단위로 쪼개져서 들어오기 때문에, 배치가 실행될 때 이전 배치의 데이터는 메모리에 없음. 따라서 배치 경계를 넘는 이전 행 참조가 불가능함.

**해결**

`anomaly_detection.py`에서 Spark 의존성을 완전히 제거하고, 순수 Python 유틸 함수 2개로 재작성.

```python
def calculate_change_rate(current_close, prev_close): ...
def is_anomaly(change_rate, threshold=3.0): ...
```

직전 분봉은 `foreachBatch` 안에서 PostgreSQL에 직접 조회해서 가져옴. 어차피 직전 분봉은 이미 DB에 저장되어 있기 때문에 별도 state 관리 없이 해결 가능.

```sql
SELECT close FROM ohlcv
WHERE code = %s AND window_start < %s
ORDER BY window_start DESC
LIMIT 1
```

이상 탐지 로직을 별도 프로세스로 분리하지 않고 `foreachBatch` 안에 통합한 이유는, 분리할 경우 폴링 주기 관리 / 중복 탐지 방지 / 프로세스 동기화 문제가 생기기 때문임. 이 규모에서는 `foreachBatch` 안에서 저장 직후 바로 처리하는 것이 더 단순하고 안전함.

---

### 8. 이상 탐지 고정 임계값 문제

**문제**

고정 3% 임계값을 모든 코인에 적용하면 변동성이 작은 BTC는 이상을 못 잡고, 변동성이 큰 DOGE는 알람이 과도하게 발생함.

**원인**

코인마다 변동성이 달라 동일한 임계값을 적용하면 코인별 특성을 반영하지 못함.

```
KRW-BTC  → 변동성 작음, 0.1%만 변해도 이상
KRW-DOGE → 변동성 큼, 1%는 정상 범위
```

**해결**

Airflow DAG로 매일 자정 코인별 최근 7일 데이터를 분석해 임계값을 재계산.

```
임계값 = 평균 변동률 + 3 * 표준편차
→ 정규분포 기준 99.7% 범위 밖이면 이상으로 판단
```

계산된 임계값은 `anomaly_threshold` 테이블에 upsert하고, Spark Streaming에서 고정값 대신 DB에서 코인별 임계값을 조회해서 사용.
