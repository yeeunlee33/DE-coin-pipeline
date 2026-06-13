# anomaly_detection.py 단위 테스트
# 실행 방법: pytest tests/test_anomaly_detection.py -v

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "processing"))

import pytest
from unittest.mock import MagicMock
from anomaly_detection import calculate_change_rate, is_anomaly, get_threshold, DEFAULT_THRESHOLD


# ── calculate_change_rate 테스트 ───────────────────────────────────────────

class TestCalculateChangeRate:

    def test_상승(self):
        # 10000 → 11000 : 10% 상승
        result = calculate_change_rate(11000, 10000)
        assert result == pytest.approx(10.0)

    def test_하락(self):
        # 10000 → 9000 : -10% 하락
        result = calculate_change_rate(9000, 10000)
        assert result == pytest.approx(-10.0)

    def test_변동없음(self):
        # 가격 동일 → 0%
        result = calculate_change_rate(10000, 10000)
        assert result == pytest.approx(0.0)

    def test_prev_close가_None(self):
        # 첫 번째 분봉이라 직전 분봉 없음 → 0.0 반환
        result = calculate_change_rate(10000, None)
        assert result == 0.0

    def test_prev_close가_0(self):
        # 0으로 나누기 방지 → 0.0 반환
        result = calculate_change_rate(10000, 0)
        assert result == 0.0

    def test_급등(self):
        # 비트코인 10만원 → 15만원 : 50% 급등
        result = calculate_change_rate(150000, 100000)
        assert result == pytest.approx(50.0)


# ── is_anomaly 테스트 ─────────────────────────────────────────────────────

class TestIsAnomaly:

    def test_임계값_초과(self):
        # 변동률 5%, 임계값 3% → 이상 탐지
        assert is_anomaly(5.0, 3.0) is True

    def test_임계값_미만(self):
        # 변동률 2%, 임계값 3% → 정상
        assert is_anomaly(2.0, 3.0) is False

    def test_임계값_정확히_일치(self):
        # 변동률 == 임계값 → 이상으로 판단 (>=)
        assert is_anomaly(3.0, 3.0) is True

    def test_급락도_이상탐지(self):
        # 변동률 -5% → 절댓값 5% → 이상 탐지
        assert is_anomaly(-5.0, 3.0) is True

    def test_소폭_하락은_정상(self):
        # 변동률 -1% → 정상
        assert is_anomaly(-1.0, 3.0) is False

    def test_기본_임계값_적용(self):
        # Airflow 미실행 시 DEFAULT_THRESHOLD(3.0) 사용 케이스
        assert is_anomaly(4.0, DEFAULT_THRESHOLD) is True
        assert is_anomaly(2.0, DEFAULT_THRESHOLD) is False


# ── get_threshold 테스트 ──────────────────────────────────────────────────

class TestGetThreshold:

    def test_DB에_임계값_있음(self):
        # DB에 KRW-BTC 임계값 2.5%가 있는 경우
        # MagicMock: 실제 DB 없이 가짜 커서를 만들어요
        # fetchone()이 (2.5,)를 반환하도록 설정
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (2.5,)

        result = get_threshold(mock_cur, "KRW-BTC")
        assert result == 2.5

    def test_DB에_임계값_없음_기본값_반환(self):
        # Airflow 아직 안 돌렸거나 신규 코인이라 DB에 없는 경우
        # fetchone()이 None을 반환 → DEFAULT_THRESHOLD(3.0) 반환해야 함
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = None

        result = get_threshold(mock_cur, "KRW-BTC")
        assert result == DEFAULT_THRESHOLD

    def test_올바른_코인으로_쿼리(self):
        # KRW-ETH로 조회할 때 실제로 KRW-ETH를 쿼리하는지 검증
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (1.8,)

        get_threshold(mock_cur, "KRW-ETH")

        # execute()가 "KRW-ETH"를 인자로 호출됐는지 확인
        call_args = mock_cur.execute.call_args
        assert "KRW-ETH" in call_args[0][1]
