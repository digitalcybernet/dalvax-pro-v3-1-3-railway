import asyncio
import aiohttp
import hmac
import hashlib
import base64
import json
import math
import logging
import sys
import os
import time as _wall_time
from datetime import datetime, timezone
import threading
import uuid
from collections import deque
from http.server import BaseHTTPRequestHandler, HTTPServer
import json as _json

# ============================================================
# DALVAX PRO v3.1.3 RAILWAY — COMPLETO E CORRIGIDO
#
# v3.1.3 changelog (sobre v3.1.2) — Fix ADX + Fix BE/amend-algo:
#
#   [FIX-ADX-01] Indicadores.adx(): recorrência Wilder corrigida.
#                Fórmula anterior:  sm = sm - (sm/p) + v           ← ERRADA
#                Fórmula correta:   sm = (sm * (p-1) + v) / p
#                Impacto: ADX inflado 14× (valores 300-500 em vez de 0-100),
#                bloqueando quase todas as entradas via ADX_MAX_FOR_MEANREV.
#   [FIX-ADX-02] Inicialização ADX corrigida: DX→ADX usa MEAN init (média dos
#                primeiros `period` DX values). TR/+DM/-DM mantêm SUM init
#                (especificação original de Wilder para cálculo dos DI).
#   [FIX-ADX-03] Clamp defensivo: se ADX > 100 (indica dado inválido) →
#                clampado a 100.0 com aviso no log. Evita bloqueio por bug futuro.
#   [FIX-ADX-04] Log detalhado de ADX: +DI, -DI, DX, ADX logados em DEBUG
#                quando ADX_FILTER_ENABLED=true para diagnóstico.
#
#   [FIX-BE-01]  OKXClient.amend_algo(): adicionado parâmetro instId (obrigatório
#                na API OKX). Ausência de instId causava HTTP 404.
#   [FIX-BE-02]  OKXClient.cancel_algo(): novo método. Cancela OCO ativo via
#                POST /api/v5/trade/cancel-algos com payload [{algoId, instId}].
#   [FIX-BE-03]  _tracked_positions: agora armazena sz, close_side, tick_sz
#                (necessários para recriar OCO após cancelamento).
#   [FIX-BE-04]  _check_break_even(): fallback cancel+recreate quando amend falha.
#                Fluxo: (1) tenta amend-algo com instId correto;
#                       (2) se falhar → cancela OCO atual;
#                       (3) recria novo OCO com SL=break_even e mesmo TP;
#                       (4) atualiza algo_id no _tracked_positions.
#                Janela desprotegida entre cancel e recreate: ~100-300ms.
#
# v3.1.2 changelog (sobre v3.1.1) — Auto Break-Even + Pump Short Exhaustion:
#   [BE-01]   OKXClient.amend_algo(): novo método para mover SL de OCO ativo
#             via /api/v5/trade/amend-algo sem cancelar o OCO.
#   [BE-02]   _tracked_positions agora armazena algo_id e be_moved flag.
#   [BE-03]   Bot._check_break_even(): monitora PnL intra-trade e move SL
#             para entry + BE_OFFSET_PCT quando PnL >= BE_TRIGGER_PCT.
#             Não executa 2x na mesma posição (flag be_moved).
#             Não reduz SL se novo valor for pior que o atual.
#   [BE-04]   Loop principal: _check_break_even() chamado ANTES do scanner.
#   [BE-05]   Config: BE_ENABLED, BE_TRIGGER_PCT, BE_OFFSET_PCT (BE_TRAIL_AFTER
#             reservado para v3.1.3 — trailing requer cancelar OCO, risco alto).
#   [PUMP-01] Bot._pump_scan(tickers): detecta ativos com variação 24h >=
#             PUMP_MIN_24H_CHANGE_PCT E volume >= PUMP_MIN_VOL_USDT.
#   [PUMP-02] _build_universe(): quando PUMP_ENABLED=true e
#             PUMP_UNIVERSE_ONLY=true, substitui o universo por pumps apenas.
#             Quando PUMP_UNIVERSE_ONLY=false, pumps são inseridos no topo.
#   [PUMP-03] _get_signal(): filtro direcional pump-aware.
#             Se PUMP_ALLOW_LONGS=false e ativo em pump → bloqueia BUY.
#             Se PUMP_REQUIRE_PUMP_FOR_SELL=true e ativo sem pump → bloqueia SELL.
#   [PUMP-04] Config: PUMP_ENABLED, PUMP_MIN_24H_CHANGE_PCT, PUMP_MIN_VOL_USDT,
#             PUMP_ALLOW_LONGS, PUMP_REQUIRE_PUMP_FOR_SELL, PUMP_UNIVERSE_ONLY.
#   [PUMP-05] _pump_set: set de instIds detectados como pump no ciclo atual,
#             compartilhado entre _pump_scan e _get_signal.
#
# v3.1.1 changelog (sobre v3.1.0) — Parâmetros de sinal configuráveis:
#   [v3.1.1-ENV]   RSI_OB, RSI_OS, RSI_LEN, BB_PERIOD, BB_STD agora
#                  configuráveis via env var (antes hardcoded). Permite
#                  ajustar sensibilidade do sinal sem redeployar o código.
#   [v3.1.1-ENV]   VOLUME_CLIMAX_MULT, PAVIO_PCT_MIN, RSI_DIVERGENCE_MIN,
#                  VOLUME_DECAY_THRESHOLD, REQUIRE_DOUBLE_PATTERN,
#                  REQUIRE_RSI_DIVERGENCE, REQUIRE_VOLUME_DECAY agora todos
#                  via env var. DALVAX_RELAX ainda funciona como atalho.
#   [v3.1.1-ENV]   BAR_INTERVAL e PATTERN_INTERVAL configuráveis via env.
#   [v3.1.1-ENV]   DOUBLE_TOP_TOLERANCE_PCT/MIN/MAX_CANDLES via env.
#
# v3.1.0 changelog (sobre v3.0.9) — Dual Mode automático:
#   [DUAL-MODE]     Dual Mode DAY ↔ POSITION implementado com correções sobre o
#                   rascunho anterior. Seletor de regime via BTC 1H (EMA200+ADX14).
#   [FIX-DM-INIT]  _eff_* e _dual_* inicializados em __init__() com defaults DAY.
#                   Rascunho anterior os colocava em _check_day_reset() com
#                   auto-referência → AttributeError no boot.
#   [FIX-DM-TOL]   _trend_filter_ok: tol corretamente definido (não auto-ref) e
#                   aplicado na comparação EMA com margem percentual.
#   [FIX-DM-ADX]   _regime_filters_ok: ADX filter desabilitado em POSITION mode
#                   (em tendência, ADX alto é desejado, não bloqueante).
#   [FIX-DM-RESTORE] _apply_mode_overlays DAY: max_positions/max_same_dir
#                   restaurados de config.* (antes eram no-op auto-referencial).
#   [FIX-DM-RESET] _check_day_reset: removido bloco dual mode (reset todo ciclo
#                   impedia qualquer mode switch de persistir).
#   [FIX-DM-VER]   Versões "v3.0.9" → "v3.1.0" em boot_snapshot/health/status.
#
# v3.0.9 changelog (sobre v3.0.8) — Auditoria técnica completa:
#   [FIX-P0-RUN]    Bot.run() estava com indent=0 (módulo global) → corrigido para
#                   indent=4 (método da classe Bot). Antes: AttributeError em toda
#                   inicialização.
#   [FIX-P0-ADX]    Indicators.adx() estava aninhado dentro de calc_atr() como
#                   closure (indent=4 após função de módulo). Movido para dentro
#                   da classe Indicators com @staticmethod correto.
#   [FIX-P0-EMA]    Indicators.ema() implementado. _trend_filter_ok() chamava
#                   Indicators.ema() que não existia → AttributeError em todo sinal
#                   com TREND_FILTER_ENABLED=True (padrão).
#   [FIX-P0-SCOPE]  atr_pct inicializada como None antes do bloco de sizing.
#                   _risk_parity_margin movida para dentro do loop por-símbolo,
#                   após _regime_filters_ok() retornar atr_pct. Antes: NameError
#                   quando NOTIONAL_USD > 0 e RISK_PARITY_ENABLED=True.
#   [FIX-P1-VER]    /api/health e /api/status atualizados de "3.0.7" para "3.0.9".
#   [FIX-P1-RESET]  _check_day_reset() agora reseta _sl_guard_until, _dd_guard_until,
#                   _sl_events, _dd_events e _pair_lock_until no virar do dia UTC.
#   [FIX-P1-RSI]    _get_signal() refatorada para retornar (signal, meta) onde meta
#                   inclui rsi_val e volume_mult. Semi-auto usa meta em vez de dir().
#   [FIX-P1-CAND]   _regime_filters_ok() aceita parâmetro c5 opcional para reutilizar
#                   candles já buscados por _get_signal(), reduzindo chamadas de API.
#   [FIX-P1-SLP]    STOP_LOSS_PCT=0.02 dead code em L246 removido. Apenas a
#                   definição via env_float (L300 original) permanece.
#   [FIX-P2-DIR]    _resolve_data_dir() agora é efetivamente usada em STATE_PATH
#                   e JOURNAL_PATH em vez de os.environ.get direto.
#   [FIX-P2-LOG]    Changelog corrigido: v3.0.8 era "sobre v3.0.7" (não v3.0.6).
#                   Features v3.0.8 documentadas no changelog.
#
# v3.0.8 changelog (sobre v3.0.7):
#   [SL-GUARD]      Stoploss Cluster Guard: pausa entradas quando múltiplos SL
#                   ocorrem em janela curta (SL_GUARD_MAX_SL / WINDOW_MIN / PAUSE_SEC).
#   [DD-GUARD]      Drawdown Window Guard: pausa quando perdas recentes superam
#                   limite percentual (DD_GUARD_PCT / WINDOW_TRADES / PAUSE_SEC).
#   [PAIR-LOCK]     Pair Performance Lock: trava par com PnL médio baixo/negativo
#                   em janela curta (PAIR_LOCK_MIN_TRADES / MAX_AVG_PNL_PCT / SEC).
#   [TREND-HTF]     Trend Filter HTF: EMA200 no timeframe 1H para evitar entradas
#                   contra-tendência (TREND_FILTER_ENABLED / HTF_BAR / EMA_LEN).
#   [REGIME]        Regime Filters: ADX e ATR% para bloquear entradas em mercado
#                   inadequado (ADX_FILTER_ENABLED / ADX_MAX_FOR_MEANREV /
#                   ATR_PCT_FILTER_ENABLED / ATR_PCT_MIN / ATR_PCT_MAX).
#   [RISK-PARITY]   Risk Parity ATR-based: ajusta margem por volatilidade do ativo
#                   (RISK_PARITY_ENABLED / TARGET_ATR_PCT / MIN_FACTOR / MAX_FACTOR).
#
# v3.0.8 changelog original (sobre v3.0.6):
#   [OPT-TICKERS]   get_tickers() chamado 1x por ciclo (era 2x).
#                   Resultado passado para _build_universe(tickers) e reutilizado
#                   no scanner do dashboard, economizando ~1 chamada de API a cada 30s.
#   [OPT-CORR]      _correlation_gate() agora usa correlação direcional efetiva:
#                   eff_corr = r × sign_open × sign_candidate
#                   Hedge (posição LONG aberta + candidato SHORT correlacionado) não é
#                   mais bloqueado — apenas acúmulo de risco na mesma direção é barrado.
#                   Parâmetro `signal` adicionado à assinatura do método.
#
# v3.0.6 changelog (sobre v3.0.5):
#   [FIX-P0-VOL]   Implementado _btc_volatility_guard(): busca candles BTC-USDT-SWAP,
#                  calcula ATR com suavização Wilder, compara ATR% com threshold e
#                  ativa pause timer (BTC_VOL_GUARD_PAUSE_SEC).
#   [FIX-P0-CORR]  Implementado _correlation_gate(): busca candles do candidato +
#                  todas as posições abertas, calcula correlação de Pearson em retornos
#                  log, bloqueia entrada se corr > CORR_THRESHOLD.
#   [FIX-P0-ATR]   calc_atr() reescrito com ATR Wilder correto: SMA inicial nos primeiros
#                  `period` TRs, depois suavização exponencial. Não mais trunca a série.
#   [FIX-P0-SEC]   API secret nunca mais logado completo — log mostra apenas primeiros 4
#                  chars + "****". Railway: defina DALVAX_API_SECRET nas variáveis.
#   [FIX-CB]       Circuit breaker diário persiste flag "halted" até virar o dia UTC
#                  (não apenas dorme 1h e retoma). Reseta automaticamente em _check_day_reset().
#   [FIX-TS]       pending_signals: mutações no loop principal agora usam _STATE_LOCK,
#                  eliminando condição de corrida com o handler HTTP.
#   [FIX-PATH]     STATE_PATH e JOURNAL_PATH usam RAILWAY_VOLUME_MOUNT_PATH se disponível,
#                  com fallback para /data e último recurso /tmp.
#   [FIX-IMPORT]   `import time` movido ao topo do arquivo (removido do interior do loop).
#   [FIX-VER]      Strings de versão corrigidas em /api/health e /api/status.
#
# v3.0.5 changelog (sobre v3.0.4):
#   [GATES]  Config de correlation gate e volatility guard adicionada (implementação faltava).
#   [SIZES]  max_same_direction gate implementado com contadores por direção.
#
# v3.0.4 changelog (sobre v3.0.3):
#   [FIX-BOOT-01] Boot log mostra raw values: DryRun=%r (raw=...) e RealTradingEnabled=%r (raw=...)
#                 MIN_RR também logado com raw. Detecta espaços invisíveis do Railway sem precisar
#                 inspecionar as variáveis — visível direto no log em < 5 segundos.
#   [FIX-ENV-02]  OKX_SIMULATED, OKX_API_KEY, OKX_SECRET_KEY, OKX_PASSPHRASE migrados para
#                 env_str() — consistência total, todos os parâmetros agora passam por .strip().
#
# v3.0.3 changelog (sobre v3.0.2):
#   [FIX-ENV-01] Helpers env_str/env_bool/env_int/env_float com .strip() obrigatório.
#                BUG CRÍTICO corrigido: "true " (espaço no Railway) virava False e
#                bloqueava todas as ordens. 24 variáveis de env migradas.
#   [FIX-LOG-01] RR check: entry_px, sl_px, tp_px com %.10g + min_rr no log.
#   [FIX-RR-01]  Guard: rr + 1e-9 < MIN_RR*0.99 com log MIN_RR_BLOCK explícito.
#   [EXIT-CLASSIFY-01] classify_exit() com tolerância EPS=0.1% — diferencia TP vs SL.
#   [EXIT-CLASSIFY-02] Watcher usa classify_exit() para classificação segura.
#
# v3.0.2 changelog (sobre v3.0.1):
#   [FIX-01] RR check log: formato %.10g para preços baixos (ex: SATS 1e-8)
#   [FIX-02] Guarda RR-07: cancela trade se rr_real < MIN_RR×0.99 (float safety)
#   [FIX-03] Aviso CRÍTICO no boot se REAL_TRADING_ENABLED=False
#   [EXIT-01] _tracked_positions: dict para rastrear posições abertas com entry_px/tp_px/sl_px
#   [EXIT-02] Registra posição em _tracked_positions após ordem confirmada + OCO
#   [EXIT-03] Watcher de fechamento no loop: detecta TP/SL executados na OKX e loga saída
#             - Diferencia TP vs SL comparando exit_px com preços esperados
#             - Grava event "position_closed" no JSONL com pnl_pct, exit_reason, entry/exit_px
#
#   [RR-01]  ATR(14) dinâmico para cálculo de SL (ATR × ATR_MULTIPLIER_SL)
#   [RR-02]  TP garantido via MIN_RR: tp_distance = sl_distance × MIN_RR
#   [RR-03]  SL_MAX_PCT: cap máximo de SL para ativos muito voláteis
#   [RR-04]  Fallback fixo (STOP_LOSS_PCT + TAKE_PROFIT_PCT) quando ATR indisponível
#   [RR-05]  STOP_LOSS_PCT e TAKE_PROFIT_PCT configuráveis via env var
#   [RR-06]  Log de RR check por trade: SL=, TP=, RR=, mode=
#   [ENV-RR] Novas env vars: DALVAX_ATR_MULTIPLIER_SL, DALVAX_MIN_RR,
#            DALVAX_SL_MAX_PCT, DALVAX_TAKE_PROFIT_PCT, DALVAX_STOP_LOSS_PCT
#   [ENV-01]  MAX_POSITIONS, LOOP_SECONDS, UNIVERSE_SIZE, MIN_VOLUME via env var
#   [ENV-02]  MAX_DAILY_LOSS_PCT, LEVERAGE, MARGIN_MODE via env var
#   [ENV-03]  SEMI_AUTO_TIMEOUT_SEC configurável via env var
#   [ENV-04]  DRY_RUN: roda tudo, não envia ordens
#   [ENV-05]  REAL_TRADING_ENABLED: chave mestra de segurança para modo real
#   [ENV-06]  SYMBOL_COOLDOWN_SEC: anti-reentrada por ativo
#   [ENV-07]  FUNDING_GATE_MODE: directional | strict | off
#   [JOURNAL] Trade Journal JSONL completo: boot_snapshot, decision, order_submit,
#             order_ack, oco_set, position_close por trade_id + clOrdId + run_id
#   [FIX-01]  tp_px/sl_px calculados ANTES do bloco semi-auto (corrige UnboundLocalError)
# Perfil padrão: moderate
# Deploy: Railway.app
# Variáveis de ambiente necessárias:
#   OKX_API_KEY, OKX_SECRET_KEY, OKX_PASSPHRASE
#   OKX_SIMULATED=1 (paper trade) ou 0 (real)
#   DALVAX_RELAX=moderate (padrão)
# ============================================================


# ═══════════════════════════════════════════════════════════════
# [FIX-ENV-01] Helpers para leitura segura de variáveis de ambiente.
# Sempre aplica .strip() para evitar que espaços acidentais no Railway
# (ex.: "true " em vez de "true") quebrem o parse booleano.
# ═══════════════════════════════════════════════════════════════
def env_str(name: str, default: str = "") -> str:
    v = os.environ.get(name, default)
    return (v if v is not None else default).strip()

def env_bool(name: str, default: bool = False) -> bool:
    v = env_str(name, "true" if default else "false").lower()
    return v in ("1", "true", "yes", "y", "on")

def env_int(name: str, default: int = 0) -> int:
    try:
        return int(env_str(name, str(default)))
    except Exception:
        return default

def env_float(name: str, default: float = 0.0) -> float:
    try:
        return float(env_str(name, str(default)))
    except Exception:
        return default


class Config:
    # ── Paths — usa RAILWAY_VOLUME_MOUNT_PATH se disponível ──────────────────
    # Prioridade: env var explícita → volume Railway → /data → /tmp (último recurso)
    @staticmethod
    def _resolve_data_dir() -> str:
        # 1. Volume Railway (RAILWAY_VOLUME_MOUNT_PATH injetado automaticamente)
        vol = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
        if vol:
            return vol
        # 2. /data — funciona se volume montado manualmente nesse path
        try:
            os.makedirs("/data", exist_ok=True)
            test = "/data/.write_test"
            with open(test, "w") as f:
                f.write("ok")
            os.remove(test)
            return "/data"
        except Exception:
            pass
        # 3. /tmp — efêmero, mas nunca falha em permissão
        return "/tmp"

    _DATA_DIR = None  # será preenchido abaixo após definição da classe

    STATE_PATH   = env_str("DALVAX_STATE_PATH", "")
    JOURNAL_PATH = env_str("DALVAX_TRADE_JOURNAL_PATH",
                           env_str("DALVAX_JOURNAL_PATH", ""))

    # ── Credenciais ───────────────────────────────────────────────────────────
    API_KEY    = ""
    SECRET_KEY = ""
    PASSPHRASE = ""
    SIMULATED  = True   # padrão seguro: simulado

    BASE_URL = "https://www.okx.com"

    # ── Timeframes ────────────────────────────────────────────────────────────
    BAR_INTERVAL     = env_str("DALVAX_BAR_INTERVAL", "5m")
    PATTERN_INTERVAL = env_str("DALVAX_PATTERN_INTERVAL", "15m")

    # ── Risco — todos configuráveis via env var ───────────────────────────────
    # [ENV-01] MAX_POSITIONS: trades simultâneos (ex: DALVAX_MAX_POSITIONS=3)
    MAX_POSITIONS      = env_int("DALVAX_MAX_POSITIONS", 3)
    # [ENV-02] Leverage e margin mode

    # ── Diversificação / Proteções adicionais ────────────────────────────────
    # Gate de correlação: bloqueia entrada se candidato for muito correlacionado com posições abertas.
    CORR_THRESHOLD     = env_float("DALVAX_CORR_THRESHOLD", 0.85)
    CORR_LOOKBACK      = env_int("DALVAX_CORR_LOOKBACK", 60)      # 30~60 candles é o recomendado
    CORR_BAR           = env_str("DALVAX_CORR_BAR", "5m")         # 1m ou 5m

    # Máximo de posições na mesma direção (ex.: 2 longs e 1 short)
    MAX_SAME_DIRECTION = env_int("DALVAX_MAX_SAME_DIRECTION", 2)

    # Volatility Guard Global (BTC): pausa entradas se ATR% do BTC "disparar"
    BTC_VOL_GUARD_ATR_PCT_MAX = env_float("DALVAX_BTC_ATR_PCT_MAX", 0.01)  # 1.0% (ajuste via env)
    BTC_VOL_GUARD_BAR         = env_str("DALVAX_BTC_ATR_BAR", "5m")
    BTC_VOL_GUARD_LOOKBACK    = env_int("DALVAX_BTC_ATR_LOOKBACK", 60)
    BTC_VOL_GUARD_PAUSE_SEC   = env_int("DALVAX_VOL_GUARD_PAUSE_SEC", 900)

    # ── Protections estilo "day trade" (inspirado em bots profissionais) ─────────
    # Stoploss Cluster Guard: se muitos SL em janela curta, pausa entradas
    SL_GUARD_MAX_SL          = env_int("DALVAX_SL_GUARD_MAX_SL", 3)            # ex.: 3 SL
    SL_GUARD_WINDOW_MIN      = env_int("DALVAX_SL_GUARD_WINDOW_MIN", 45)       # em minutos
    SL_GUARD_PAUSE_SEC       = env_int("DALVAX_SL_GUARD_PAUSE_SEC", 900)       # pausa (segundos)

    # Drawdown Window Guard: se drawdown recente exceder limite, pausa entradas
    DD_GUARD_PCT             = env_float("DALVAX_DD_GUARD_PCT", 0.02)          # ex.: 2% da equity do dia
    DD_GUARD_WINDOW_TRADES   = env_int("DALVAX_DD_GUARD_WINDOW_TRADES", 20)    # últimas N saídas
    DD_GUARD_PAUSE_SEC       = env_int("DALVAX_DD_GUARD_PAUSE_SEC", 1200)

    # Pair Performance Lock: trava par "ruim" por um tempo
    PAIR_LOCK_MIN_TRADES     = env_int("DALVAX_PAIR_LOCK_MIN_TRADES", 3)
    PAIR_LOCK_MAX_AVG_PNL_PCT= env_float("DALVAX_PAIR_LOCK_MAX_AVG_PNL_PCT", 0.15)  # avg <= 0.15% -> lock
    PAIR_LOCK_SEC            = env_int("DALVAX_PAIR_LOCK_SEC", 3600)

    # ── Dual Mode (DAY ↔ POSITION) ────────────────────────────────────────────
    # [DUAL-MODE] Seletor de regime: DAY (mean-reversion) ↔ POSITION (trend-follow)
    # Habilitar: DALVAX_DUAL_MODE_ENABLED=true
    DUAL_MODE_ENABLED       = env_bool("DALVAX_DUAL_MODE_ENABLED", False)

    # Regime detector (termômetro global — recomendado BTC 1H)
    REGIME_SYMBOL           = env_str("DALVAX_REGIME_SYMBOL", "BTC-USDT-SWAP")
    REGIME_BAR              = env_str("DALVAX_REGIME_BAR", "1H")
    REGIME_EMA_LEN          = env_int("DALVAX_REGIME_EMA_LEN", 200)
    REGIME_ADX_PERIOD       = env_int("DALVAX_REGIME_ADX_PERIOD", 14)
    REGIME_ADX_TREND_MIN    = env_float("DALVAX_REGIME_ADX_TREND_MIN", 28.0)
    REGIME_CONFIRM_CYCLES   = env_int("DALVAX_REGIME_CONFIRM_CYCLES", 3)

    # Overlays — DAY (mean-reversion: mais trades, alvos menores, filtros permissivos)
    DAY_MIN_RR              = env_float("DALVAX_DAY_MIN_RR", 1.3)
    DAY_ATR_MULT_SL         = env_float("DALVAX_DAY_ATR_MULT_SL", 1.2)
    DAY_SL_MAX_PCT          = env_float("DALVAX_DAY_SL_MAX_PCT", 0.015)
    DAY_COOLDOWN_SEC        = env_int("DALVAX_DAY_COOLDOWN_SEC", 600)
    DAY_CORR_THRESHOLD      = env_float("DALVAX_DAY_CORR_THRESHOLD", 0.95)
    DAY_TREND_TOL_PCT       = env_float("DALVAX_DAY_TREND_TOL_PCT", 0.003)  # 0.3% buffer na EMA

    # Overlays — POSITION (trend-follow: menos trades, alvos maiores, filtros rígidos)
    POS_MIN_RR              = env_float("DALVAX_POS_MIN_RR", 2.8)
    POS_ATR_MULT_SL         = env_float("DALVAX_POS_ATR_MULT_SL", 1.9)
    POS_SL_MAX_PCT          = env_float("DALVAX_POS_SL_MAX_PCT", 0.03)
    POS_COOLDOWN_SEC        = env_int("DALVAX_POS_COOLDOWN_SEC", 2400)
    POS_CORR_THRESHOLD      = env_float("DALVAX_POS_CORR_THRESHOLD", 0.88)
    POS_TREND_TOL_PCT       = env_float("DALVAX_POS_TREND_TOL_PCT", 0.0)    # sem tolerância — rígido

    # Trend/Regime filters (day trade)
    TREND_FILTER_ENABLED     = env_bool("DALVAX_TREND_FILTER_ENABLED", True)
    TREND_HTF_BAR            = env_str("DALVAX_TREND_HTF_BAR", "1H")           # 1H por padrão
    TREND_EMA_LEN            = env_int("DALVAX_TREND_EMA_LEN", 200)            # EMA200 HTF

    ADX_FILTER_ENABLED       = env_bool("DALVAX_ADX_FILTER_ENABLED", True)
    ADX_LEN                  = env_int("DALVAX_ADX_LEN", 14)
    ADX_MAX_FOR_MEANREV      = env_float("DALVAX_ADX_MAX_FOR_MEANREV", 25.0)  # se acima disso, mercado tende a "trend"

    ATR_PCT_FILTER_ENABLED   = env_bool("DALVAX_ATR_PCT_FILTER_ENABLED", True)
    ATR_PCT_MIN              = env_float("DALVAX_ATR_PCT_MIN", 0.002)         # 0.2%
    ATR_PCT_MAX              = env_float("DALVAX_ATR_PCT_MAX", 0.02)          # 2.0%

    # Risk Parity (ajusta margem por trade por volatilidade do ativo)
    RISK_PARITY_ENABLED      = env_bool("DALVAX_RISK_PARITY_ENABLED", True)
    RISK_PARITY_TARGET_ATR_PCT = env_float("DALVAX_RISK_PARITY_TARGET_ATR_PCT", 0.01)  # 1.0%
    RISK_PARITY_MIN_FACTOR     = env_float("DALVAX_RISK_PARITY_MIN_FACTOR", 0.5)
    RISK_PARITY_MAX_FACTOR     = env_float("DALVAX_RISK_PARITY_MAX_FACTOR", 1.5)


    LEVERAGE           = env_int("DALVAX_LEVERAGE", 5)
    MARGIN_MODE        = env_str("DALVAX_MARGIN_MODE", "isolated")
    # [ENV-02] Circuit breaker diário
    MAX_DAILY_LOSS_PCT = env_float("DALVAX_MAX_DAILY_LOSS_PCT", 0.03)

    # ── Perfil de trading ─────────────────────────────────────────────────────
    RELAX_PROFILE          = env_str("DALVAX_RELAX", "moderate").lower()
    # [v3.1.1] Parâmetros de sinal agora configuráveis via env var
    VOLUME_CLIMAX_MULT     = env_float("DALVAX_VOLUME_CLIMAX_MULT", 1.6)
    PAVIO_PCT_MIN          = env_float("DALVAX_PAVIO_PCT_MIN", 0.35)
    RSI_DIVERGENCE_MIN     = env_float("DALVAX_RSI_DIVERGENCE_MIN", 3.0)
    VOLUME_DECAY_THRESHOLD = env_float("DALVAX_VOLUME_DECAY_THRESHOLD", 0.92)
    REQUIRE_DOUBLE_PATTERN = env_bool("DALVAX_REQUIRE_DOUBLE_PATTERN", True)
    REQUIRE_RSI_DIVERGENCE = env_bool("DALVAX_REQUIRE_RSI_DIVERGENCE", True)
    REQUIRE_VOLUME_DECAY   = env_bool("DALVAX_REQUIRE_VOLUME_DECAY", True)

    RSI_LEN = env_int("DALVAX_RSI_LEN", 14)
    RSI_OB  = env_float("DALVAX_RSI_OB", 72.0)   # Overbought threshold (padrão: 72)
    RSI_OS  = env_float("DALVAX_RSI_OS", 28.0)   # Oversold threshold  (padrão: 28)

    BB_PERIOD = env_int("DALVAX_BB_PERIOD", 20)
    BB_STD    = env_float("DALVAX_BB_STD", 2.2)   # Desvios padrão da BB

    DOUBLE_TOP_TOLERANCE_PCT = env_float("DALVAX_DOUBLE_TOP_TOL_PCT", 0.015)
    DOUBLE_TOP_MIN_CANDLES   = env_int("DALVAX_DOUBLE_TOP_MIN_CANDLES", 6)
    DOUBLE_TOP_MAX_CANDLES   = env_int("DALVAX_DOUBLE_TOP_MAX_CANDLES", 48)

    # ── Funding gate ──────────────────────────────────────────────────────────
    # MAX_FUNDING_RATE: limite direcional de funding.
    # Configurável via DALVAX_MAX_FUNDING_RATE (padrão 0.005 = 0.5%)
    MAX_FUNDING_RATE = env_float("DALVAX_MAX_FUNDING_RATE", 0.005)
    # [ENV-07] FUNDING_GATE_MODE: directional | strict | off
    #   directional (padrão): bloqueia BUY se funding muito positivo / SELL se muito negativo
    #   strict: bloqueia se |funding| > MAX em qualquer direção
    #   off: não aplica filtro de funding (útil para debug)
    FUNDING_GATE_MODE = env_str("DALVAX_FUNDING_GATE_MODE", "directional").lower()

    # ── Custos OKX ────────────────────────────────────────────────────────────
    TAKER_FEE_PCT = 0.001   # 0.1% round-trip
    SLIPPAGE_PCT  = 0.001   # 0.1% estimado

    # ── Anti-microtrade guard (AT1)
    # Evita trades com alvo tão curto que a taxa/slippage come o edge.
    # Unidade: PERCENT (ex.: 0.40 = 0.40%)
    MIN_EDGE_PCT = env_float("DALVAX_MIN_EDGE_PCT", 0.40)


    # ── Signal inversion toggle (AT1)
    # Se true: BUY vira SELL e SELL vira BUY (sem alterar análise técnica)
    INVERT_SIGNAL = env_bool("DALVAX_INVERT_SIGNAL", False)
    # ── Risk/Reward — ATR dinâmico + fallback fixo ────────────────────────────
    # [RR-01] Modo de TP/SL: "atr_rr" (dinâmico) ou "fixed" (fallback)
    # atr_rr: SL = ATR(14) × ATR_MULTIPLIER_SL; TP = SL × MIN_RR
    # fixed:  SL = STOP_LOSS_PCT × px; TP = TAKE_PROFIT_PCT × px
    TP_MODE = env_str("DALVAX_TP_MODE", "atr_rr").lower()

    # [RR-01] Multiplicador do ATR para o Stop Loss
    # ATR_MULT=1.2 + MIN_RR=2.0 → RR efetivo ~1.25 após fees a 5x
    ATR_MULTIPLIER_SL = env_float("DALVAX_ATR_MULTIPLIER_SL", 1.2)

    # [RR-02] RR mínimo garantido: TP = SL × MIN_RR
    # Recomendado: 2.0 (para RR efetivo ≥ 1.2 após fees a 5x)
    MIN_RR = env_float("DALVAX_MIN_RR", 2.0)

    # [RR-03] Cap máximo do SL em % do preço (protege contra ATR gigante)
    # Ex: 0.015 = 1.5% máximo de movimento no preço para o SL
    SL_MAX_PCT = env_float("DALVAX_SL_MAX_PCT", 0.015)

    # [RR-04/05] Fallback fixo — usado quando ATR indisponível (ou TP_MODE=fixed)
    # Valores do Relatório 2: SL=1.2%, TP=2.4% → RR efetivo ~1.25 após fees
    STOP_LOSS_PCT   = env_float("DALVAX_STOP_LOSS_PCT", 0.012)
    TAKE_PROFIT_PCT = env_float("DALVAX_TAKE_PROFIT_PCT", 0.024)
    # Notional fixo por operação (margem = NOTIONAL_USD, nocional = NOTIONAL_USD * LEVERAGE)
    # Configurável via DALVAX_NOTIONAL_USD (padrão: 0 = calculado via availBal)
    NOTIONAL_USD = env_float("DALVAX_NOTIONAL_USD", 0.0)

    # ── Universo ──────────────────────────────────────────────────────────────
    # [ENV-01] Configuráveis via env var
    MIN_VOLUME_24H_USDT = env_float("DALVAX_MIN_VOLUME_24H_USDT", 5000000.0)
    MAX_UNIVERSE_SIZE   = env_int("DALVAX_MAX_UNIVERSE_SIZE", 50)

    # ── Auto Break-Even (v3.1.2) ──────────────────────────────────────────────
    # Quando o PnL intra-trade atinge BE_TRIGGER_PCT, o SL é movido para
    # entry + BE_OFFSET_PCT (LONG) ou entry - BE_OFFSET_PCT (SHORT).
    # Garante saída no zero ou pequeno lucro mesmo em reversões rápidas.
    # BE_TRAIL_AFTER reservado para v3.1.3 (requer cancelar OCO — risco alto).
    BE_ENABLED      = env_bool("DALVAX_BE_ENABLED",      False)
    BE_TRIGGER_PCT  = env_float("DALVAX_BE_TRIGGER_PCT", 0.006)   # 0.6% lucro ativa BE
    BE_OFFSET_PCT   = env_float("DALVAX_BE_OFFSET_PCT",  0.001)   # trava +0.1% acima da entry

    # ── Pump / Short Exhaustion Module (v3.1.2) ───────────────────────────────
    # Detecta ativos em pump extremo (alta 24h >= threshold) e aplica
    # filtro direcional: só SELL permitido em pumps, BUY bloqueado.
    # Stratégia: blow-off top fade / mean-reversion pós-exaustão.
    PUMP_ENABLED                = env_bool("DALVAX_PUMP_ENABLED",                False)
    PUMP_MIN_24H_CHANGE_PCT     = env_float("DALVAX_PUMP_MIN_24H_CHANGE_PCT",    0.20)   # +20% em 24h
    PUMP_MIN_VOL_USDT           = env_float("DALVAX_PUMP_MIN_VOL_USDT",          20_000_000.0)
    PUMP_ALLOW_LONGS            = env_bool("DALVAX_PUMP_ALLOW_LONGS",            False)  # bloqueia BUY em pump
    PUMP_REQUIRE_PUMP_FOR_SELL  = env_bool("DALVAX_PUMP_REQUIRE_PUMP_FOR_SELL",  True)   # só SELL se houve pump
    PUMP_UNIVERSE_ONLY          = env_bool("DALVAX_PUMP_UNIVERSE_ONLY",          False)  # só pumps no universo

    # ── Rate limit / loop ─────────────────────────────────────────────────────
    MAX_CONCURRENT_REQUESTS = 15
    # [ENV-01] Velocidade do loop configurável
    LOOP_SECONDS = env_int("DALVAX_LOOP_SECONDS", 30)

    # ── Modo de execução ──────────────────────────────────────────────────────
    # [ENV-03] Timeout do semi-auto em segundos (padrão 120s = 2 min)
    SEMI_AUTO_TIMEOUT_SEC = env_int("DALVAX_SEMI_AUTO_TIMEOUT_SEC", 120)

    # ── Cooldown por ativo ────────────────────────────────────────────────────
    # [ENV-06] Tempo mínimo entre trades do mesmo ativo (padrão 900s = 15 min)
    SYMBOL_COOLDOWN_SEC = env_int("DALVAX_SYMBOL_COOLDOWN_SEC", 900)

    # ── Segurança ─────────────────────────────────────────────────────────────
    # [ENV-04] DRY_RUN: roda tudo, calcula sizing e TP/SL, mas NÃO envia ordens
    DRY_RUN = env_bool("DALVAX_DRY_RUN", default=False)
    # [ENV-05] REAL_TRADING_ENABLED: chave mestra para modo real
    #   Mesmo com OKX_SIMULATED=0, se false → não envia ordens (modo observação)
    REAL_TRADING_ENABLED = env_bool("DALVAX_REAL_TRADING_ENABLED", default=True)

    @classmethod
    def apply_relax_profile(cls):
        p = cls.RELAX_PROFILE

        if p == "strict":
            cls.VOLUME_CLIMAX_MULT     = 2.0
            cls.PAVIO_PCT_MIN          = 0.40
            cls.RSI_DIVERGENCE_MIN     = 5.0
            cls.VOLUME_DECAY_THRESHOLD = 0.85
            cls.REQUIRE_DOUBLE_PATTERN = True
            cls.REQUIRE_RSI_DIVERGENCE = True
            cls.REQUIRE_VOLUME_DECAY   = True

        elif p == "aggressive":
            cls.VOLUME_CLIMAX_MULT     = 1.3
            cls.PAVIO_PCT_MIN          = 0.30
            cls.RSI_DIVERGENCE_MIN     = 2.0
            cls.VOLUME_DECAY_THRESHOLD = 0.98
            cls.REQUIRE_DOUBLE_PATTERN = False
            cls.REQUIRE_RSI_DIVERGENCE = False
            cls.REQUIRE_VOLUME_DECAY   = False

        else:
            # moderate (padrão)
            cls.RELAX_PROFILE          = "moderate"
            cls.VOLUME_CLIMAX_MULT     = 1.6
            cls.PAVIO_PCT_MIN          = 0.35
            cls.RSI_DIVERGENCE_MIN     = 3.0
            cls.VOLUME_DECAY_THRESHOLD = 0.92
            cls.REQUIRE_DOUBLE_PATTERN = True
            cls.REQUIRE_RSI_DIVERGENCE = True
            cls.REQUIRE_VOLUME_DECAY   = True

    @classmethod
    def load_secrets(cls):
        """
        Carrega credenciais OKX de variáveis de ambiente (Railway).
        Compatível com nomes antigos e novos para evitar quebra após migrações:
          - OKX_API_KEY
          - OKX_SECRET_KEY / OKX_API_SECRET / OKX_SECRET / SECRET_KEY
          - OKX_PASSPHRASE / OKX_API_PASSPHRASE / PASSPHRASE

        Segurança:
          - Sempre aplica .strip() via env_str()
          - Em modo REAL (OKX_SIMULATED=0): exige as 3 credenciais.
          - Em modo SIMULADO: também exige (OKX ainda requer key para paper), mas a
            mensagem de erro é mais clara.
        """
        # Variáveis de ambiente (Railway usa isso)
        env_key = (
            env_str("OKX_API_KEY") or env_str("API_KEY")
        )
        env_secret = (
            env_str("OKX_SECRET_KEY")
            or env_str("OKX_API_SECRET")
            or env_str("OKX_SECRET")
            or env_str("SECRET_KEY")
        )
        env_pass = (
            env_str("OKX_PASSPHRASE")
            or env_str("OKX_API_PASSPHRASE")
            or env_str("PASSPHRASE")
        )

        sim_env = env_str("OKX_SIMULATED") or env_str("SIMULATED")
        if sim_env:
            cls.SIMULATED = sim_env.lower() in ("1", "true", "yes", "y", "on")

        # API Safety Gate: se faltar credencial, falha no boot (melhor que operar "meio cego")
        if not (env_key and env_secret and env_pass):
            mode = "SIMULADO" if cls.SIMULATED else "REAL"
            print(f"❌ ERRO: Credenciais OKX ausentes ({mode}).")
            print("   Necessário configurar no Railway → Variables (Name/Value):")
            print("   - OKX_API_KEY")
            print("   - OKX_SECRET_KEY  (ou OKX_API_SECRET)")
            print("   - OKX_PASSPHRASE  (ou OKX_API_PASSPHRASE)")
            raise EnvironmentError("Credenciais OKX ausentes nas variáveis de ambiente.")

        cls.API_KEY    = env_key.strip()
        cls.SECRET_KEY = env_secret.strip()
        cls.PASSPHRASE = env_pass.strip()

        # Nunca logar secrets — apenas confirmação
        print("✅ Credenciais OKX carregadas das variáveis de ambiente")


# ── Inicializar paths usando _resolve_data_dir() (FIX-P2-DIR) ────────────────
Config._DATA_DIR = Config._resolve_data_dir()
if not Config.STATE_PATH:
    Config.STATE_PATH = os.path.join(Config._DATA_DIR, "dalvax_state.json")
if not Config.JOURNAL_PATH:
    Config.JOURNAL_PATH = os.path.join(Config._DATA_DIR, "dalvax_trades.jsonl")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],   # Railway: forçar stdout (evita INFO virar ERROR)
)
logger = logging.getLogger("DALVAX")


# ══════════════════════════════════════════════════════════════════════════════
# TRADE JOURNAL — JSONL (1 evento JSON por linha)
# ──────────────────────────────────────────────────────────────────────────────
# Cada chamada a log_event() grava 1 linha em:
#   - stdout  (Railway captura)
#   - arquivo JOURNAL_PATH (padrão /tmp/dalvax_trades.jsonl)
#
# Campos sempre presentes: ts, run_id, event
# Campos por evento: ver constantes JOURNAL_* abaixo
# ══════════════════════════════════════════════════════════════════════════════


def ensure_parent_dir(path: str):
    """Garante que o diretório pai exista (útil em containers/Railway)."""
    try:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
    except Exception:
        pass

RUN_ID = uuid.uuid4().hex[:12]   # id único desta execução do processo

_journal_lock = threading.Lock()

def log_event(event: str, **fields):
    """Grava 1 evento JSONL no journal e no logger padrão."""
    payload = {
        "ts":     datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "run_id": RUN_ID,
        "event":  event,
        **fields,
    }
    line = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    # stdout — Railway indexa por linha
    print(f"[JOURNAL] {line}", flush=True)
    # arquivo local (perdido no restart do Railway — mas útil para sessão)
    try:
        with _journal_lock:
            ensure_parent_dir(Config.JOURNAL_PATH)
            with open(Config.JOURNAL_PATH, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception:
        pass  # nunca deve travar o bot por falha de I/O


# ── Helpers ───────────────────────────────────────────────────────────────────
def iso_ts():
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def today_utc_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def round_to_tick(price, tick_sz):
    if tick_sz <= 0:
        return round(price, 6)
    decimals = max(0, -int(math.floor(math.log10(tick_sz))))
    factor = 10 ** decimals
    return math.floor(price * factor / (tick_sz * factor)) * tick_sz * factor / factor


def round_to_lot(size, lot_sz):
    if lot_sz <= 0:
        return int(size)
    return int(math.floor(size / lot_sz) * lot_sz)


def calc_atr(candles: list, period: int = 14) -> float | None:
    """
    Calcula ATR(period) com suavização de Wilder a partir de candles OKX.
    Formato OKX: [ts, open, high, low, close, vol, volCcy] (ordem do mais recente ao mais antigo).
    DEVE receber candles já em ordem cronológica (reversed antes de chamar).

    Algoritmo Wilder:
      - TRs para todos os candles disponíveis (i ≥ 1)
      - ATR inicial = SMA dos primeiros `period` TRs
      - ATR[i] = (ATR[i-1] * (period-1) + TR[i]) / period

    [FIX-P0-ATR] Versão anterior truncava a série em period+5 candles
    (min(len, period+5)), ignorando candles mais recentes e distorcendo SL/TP.
    """
    try:
        if len(candles) < period + 1:
            return None
        # Calcula TR para cada candle a partir do 2º
        trs = []
        for i in range(1, len(candles)):
            hi   = float(candles[i][2])
            lo   = float(candles[i][3])
            prev = float(candles[i - 1][4])
            trs.append(max(hi - lo, abs(hi - prev), abs(lo - prev)))
        if len(trs) < period:
            return None
        # ATR inicial = SMA dos primeiros `period` TRs
        atr = sum(trs[:period]) / period
        # Suavização exponencial de Wilder para os TRs restantes
        for tr in trs[period:]:
            atr = (atr * (period - 1) + tr) / period
        return atr
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# EXCHANGE CLIENT
# ══════════════════════════════════════════════════════════════════════════════
class ExchangeClient:
    def __init__(self, config):
        self.config = config
        self.session = None
        self._rate_limiter     = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)
        self._instrument_cache = {}

    async def init_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            try:
                await self.session.close()
            except Exception:
                pass

    async def _request(self, method, path, params=None, data=None, auth=True):
        await self.init_session()

        request_path = path
        if method == "GET" and params:
            query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
            request_path = f"{path}?{query}"

        url  = f"{self.config.BASE_URL}{request_path}"
        body = json.dumps(data) if data else ""

        headers = {"Content-Type": "application/json"}
        if auth:
            ts      = iso_ts()
            prehash = f"{ts}{method}{request_path}{body}"
            sig     = base64.b64encode(
                hmac.new(
                    self.config.SECRET_KEY.encode(),
                    prehash.encode(),
                    hashlib.sha256,
                ).digest()
            ).decode()
            headers.update({
                "OK-ACCESS-KEY":       self.config.API_KEY,
                "OK-ACCESS-SIGN":      sig,
                "OK-ACCESS-TIMESTAMP": ts,
                "OK-ACCESS-PASSPHRASE": self.config.PASSPHRASE,
                "x-simulated-trading": "1" if self.config.SIMULATED else "0",
            })

        async with self._rate_limiter:
            try:
                async with self.session.request(
                    method, url,
                    json=data if method != "GET" else None,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as resp:
                    res = await resp.json()
                    if str(res.get("code")) == "0":
                        return res.get("data")
                    logger.warning(
                        "API %s %s → code=%s msg=%s",
                        method, path, res.get("code"), res.get("msg"),
                    )
                    return None
            except asyncio.TimeoutError:
                logger.error("Timeout %s %s", method, path)
                return None
            except Exception as exc:
                logger.error("Request error %s %s: %s", method, path, exc)
                return None

    # ── GET endpoints ─────────────────────────────────────────────────────────
    async def get_balance(self):
        return await self._request(
            "GET", "/api/v5/account/balance",
            params={"ccy": "USDT"},
        )

    async def get_positions(self):
        return await self._request(
            "GET", "/api/v5/account/positions",
            params={"instType": "SWAP"},
        )

    async def get_tickers(self):
        return await self._request(
            "GET", "/api/v5/market/tickers",
            params={"instType": "SWAP"},
            auth=False,
        )

    async def get_candles(self, instId, bar, limit="120"):
        return await self._request(
            "GET", "/api/v5/market/candles",
            params={"instId": instId, "bar": bar, "limit": limit},
            auth=False,
        )

    async def get_mark_price(self, instId):
        data = await self._request(
            "GET", "/api/v5/public/mark-price",
            params={"instType": "SWAP", "instId": instId},
            auth=False,
        )
        if data:
            return float(data[0].get("markPx", 0))
        return None

    async def get_funding_rate(self, instId):
        data = await self._request(
            "GET", "/api/v5/public/funding-rate",
            params={"instId": instId},
            auth=False,
        )
        if data:
            return float(data[0].get("fundingRate", 0))
        return None

    async def get_instrument_info(self, instId):
        if instId in self._instrument_cache:
            return self._instrument_cache[instId]

        data = await self._request(
            "GET", "/api/v5/public/instruments",
            params={"instType": "SWAP", "instId": instId},
            auth=False,
        )
        if data:
            inst = data[0]
            info = {
                "ctVal":    float(inst.get("ctVal",  1)),
                "tickSz":   float(inst.get("tickSz", 0.0001)),
                "lotSz":    float(inst.get("lotSz",  1)),
                "minSz":    float(inst.get("minSz",  1)),
            }
            self._instrument_cache[instId] = info
            return info
        return None

    async def get_account_config(self):
        return await self._request("GET", "/api/v5/account/config")

    # ── SET / Orders ──────────────────────────────────────────────────────────
    async def set_position_mode(self):
        data = await self.get_account_config()
        if not data:
            return False

        pos_mode = data[0].get("posMode", "")
        if pos_mode == "net_mode":
            logger.info("Position mode: net_mode ✅")
            return True

        logger.warning("posMode=%s — alterando para net_mode...", pos_mode)
        result = await self._request(
            "POST", "/api/v5/account/set-position-mode",
            data={"posMode": "net_mode"},
        )
        if result is not None:
            logger.info("Position mode → net_mode ✅")
            return True

        logger.error("❌ Falha ao setar net_mode. Verifique a conta OKX.")
        return False

    async def set_leverage(self, instId):
        data = {
            "instId":  instId,
            "lever":   str(self.config.LEVERAGE),
            "mgnMode": "isolated",
        }
        result = await self._request("POST", "/api/v5/account/set-leverage", data=data)
        return result is not None

    async def place_order(self, instId, side, sz):
        data = {
            "instId":  instId,
            "tdMode":  "isolated",
            "side":    side,
            "ordType": "market",
            "sz":      str(sz),
            "posSide": "net",
        }
        return await self._request("POST", "/api/v5/trade/order", data=data)

    async def place_oco(self, instId, side, sz, tp_px, sl_px, tick_sz):
        data = {
            "instId":  instId,
            "tdMode":  "isolated",
            "side":    side,
            "ordType": "oco",
            "sz":      str(sz),
            "posSide": "net",
        }
        if tp_px is not None:
            data["tpTriggerPx"]     = str(round_to_tick(tp_px, tick_sz))
            data["tpOrdPx"]         = "-1"
            data["tpTriggerPxType"] = "mark"

        if sl_px is not None:
            data["slTriggerPx"]     = str(round_to_tick(sl_px, tick_sz))
            data["slOrdPx"]         = "-1"
            data["slTriggerPxType"] = "mark"

        return await self._request("POST", "/api/v5/trade/order-algo", data=data)

    async def cancel_algo(self, inst_id: str, algo_id: str) -> bool:
        """
        [FIX-BE-02] Cancela um OCO/algo ativo via /api/v5/trade/cancel-algos.
        O endpoint aceita uma lista de ordens, aqui usamos sempre um único item.
        Retorna True se a OKX aceitou o cancelamento.
        """
        try:
            result = await self._request(
                "POST", "/api/v5/trade/cancel-algos",
                data=[{"algoId": algo_id, "instId": inst_id}],
            )
            return bool(result)
        except Exception as exc:
            logger.debug("cancel_algo error (algoId=%s instId=%s): %s", algo_id, inst_id, exc)
            return False

    async def amend_algo(
        self,
        inst_id: str,
        algo_id: str,
        new_sl_px: str,
        new_tp_px: str | None = None,
    ) -> bool:
        """
        [FIX-BE-01] Altera o SL (e opcionalmente o TP) de um OCO ativo.
        Usa POST /api/v5/trade/amend-algo.

        Fix v3.1.3: adicionado instId ao payload (campo obrigatório na OKX API).
        A ausência de instId causava HTTP 404 em todas as chamadas no v3.1.2.
        """
        data: dict = {
            "algoId":          algo_id,
            "instId":          inst_id,    # [FIX-BE-01] obrigatório na OKX API
            "slTriggerPx":     new_sl_px,
            "slTriggerPxType": "mark",
        }
        if new_tp_px is not None:
            data["tpTriggerPx"]     = new_tp_px
            data["tpTriggerPxType"] = "mark"
        try:
            result = await self._request("POST", "/api/v5/trade/amend-algo", data=data)
            return bool(result)
        except Exception as exc:
            logger.debug("amend_algo error (algoId=%s instId=%s): %s", algo_id, inst_id, exc)
            return False


# ══════════════════════════════════════════════════════════════════════════════
# INDICATORS
# ══════════════════════════════════════════════════════════════════════════════
class Indicators:

    @staticmethod
    def bb(values, period, std_dev):
        if len(values) < period:
            return None, None, None
        sma      = sum(values[-period:]) / period
        variance = sum((x - sma) ** 2 for x in values[-period:]) / period
        std      = math.sqrt(variance)
        return sma, sma + std_dev * std, sma - std_dev * std

    @staticmethod
    def rsi(prices, period=14):
        if len(prices) < period + 1:
            return []

        deltas = [prices[i + 1] - prices[i] for i in range(len(prices) - 1)]
        up     = sum(d for d in deltas[:period] if d >= 0) / period
        down   = sum(-d for d in deltas[:period] if d < 0) / period

        def _rsi(u, d):
            if d == 0:
                return 100.0
            return 100.0 - 100.0 / (1.0 + u / d)

        result = [_rsi(up, down)]
        for d in deltas[period:]:
            up   = (up   * (period - 1) + (d  if d > 0 else 0)) / period
            down = (down * (period - 1) + (-d if d < 0 else 0)) / period
            result.append(_rsi(up, down))

        return result

    @staticmethod
    def find_double_top(highs, closes, tolerance_pct, min_gap, max_gap):
        if len(highs) < min_gap + 4:
            return False

        peaks = []
        for i in range(2, len(highs) - 2):
            if (highs[i] >= highs[i - 1]
                    and highs[i] >= highs[i - 2]
                    and highs[i] >= highs[i + 1]
                    and highs[i] >= highs[i + 2]):
                peaks.append((i, highs[i]))

        if len(peaks) < 2:
            return False

        for j in range(len(peaks) - 1, 0, -1):
            for k in range(j - 1, -1, -1):
                idx2, px2 = peaks[j]
                idx1, px1 = peaks[k]
                gap = idx2 - idx1

                if gap < min_gap or gap > max_gap:
                    continue

                if abs(px1 - px2) / max(px1, px2) > tolerance_pct:
                    continue

                if closes[-1] < max(px1, px2) * 0.99:
                    return True

        return False

    @staticmethod
    def find_double_bottom(lows, closes, tolerance_pct, min_gap, max_gap):
        if len(lows) < min_gap + 4:
            return False

        valleys = []
        for i in range(2, len(lows) - 2):
            if (lows[i] <= lows[i - 1]
                    and lows[i] <= lows[i - 2]
                    and lows[i] <= lows[i + 1]
                    and lows[i] <= lows[i + 2]):
                valleys.append((i, lows[i]))

        if len(valleys) < 2:
            return False

        for j in range(len(valleys) - 1, 0, -1):
            for k in range(j - 1, -1, -1):
                idx2, px2 = valleys[j]
                idx1, px1 = valleys[k]
                gap = idx2 - idx1

                if gap < min_gap or gap > max_gap:
                    continue

                if abs(px1 - px2) / max(px1, px2) > tolerance_pct:
                    continue

                if closes[-1] > min(px1, px2) * 1.01:
                    return True

        return False

    @staticmethod
    def check_rsi_divergence(prices, rsi_vals, direction, min_div):
        n = min(30, len(prices), len(rsi_vals))
        if n < 20:
            return False

        p  = prices[-n:]
        r  = rsi_vals[-n:]
        h  = n // 2
        p1, p2 = p[:h], p[h:]
        r1, r2 = r[:h], r[h:]

        if direction == "bearish":
            return max(p2) >= max(p1) * 0.995 and (max(r1) - max(r2)) >= min_div

        if direction == "bullish":
            return min(p2) <= min(p1) * 1.005 and (min(r2) - min(r1)) >= min_div

        return False

    @staticmethod
    def check_volume_decay(volumes, threshold):
        n = min(30, len(volumes))
        if n < 20:
            return False
        recent = volumes[-n:]
        h  = n // 2
        v1 = sum(recent[:h]) / h
        v2 = sum(recent[h:]) / h
        return v2 < v1 * threshold

    @staticmethod
    def ema(values: list, period: int) -> float | None:
        """
        [FIX-P0-EMA] EMA(period) com suavização exponencial padrão.
        SMA inicial nos primeiros `period` valores, depois k = 2/(period+1).
        Retorna o último valor ou None se dados insuficientes.
        """
        if not values or len(values) < period:
            return None
        k = 2.0 / (period + 1)
        ema_val = sum(values[:period]) / period
        for v in values[period:]:
            ema_val = v * k + ema_val * (1 - k)
        return ema_val

    @staticmethod
    def adx(highs: list, lows: list, closes: list, period: int = 14) -> float | None:
        """
        [FIX-ADX-01/02] ADX com suavização Wilder corrigida.

        BUG v3.1.2: recorrência `sm = sm - sm/p + v` inflava ADX ~14×
        (valores 300-500). Correto: `sm = (sm*(p-1) + v) / p`.

        Inicialização:
          - TR/+DM/-DM: SUM dos primeiros `period` valores (spec original Wilder para DI).
          - DX → ADX: MEAN dos primeiros `period` DX values (spec original Wilder para ADX).
        """
        n = min(len(highs), len(lows), len(closes))
        if n < period + 2:
            return None

        trs, plus_dm, minus_dm = [], [], []
        for i in range(1, n):
            h, lo, pc = highs[i], lows[i], closes[i - 1]
            trs.append(max(h - lo, abs(h - pc), abs(lo - pc)))
            up   = highs[i] - highs[i - 1]
            down = lows[i - 1] - lows[i]
            plus_dm.append(up   if (up > down   and up   > 0) else 0.0)
            minus_dm.append(down if (down > up  and down > 0) else 0.0)

        def _wilder_sum(vals, p):
            """SUM init + corrected recurrence — para TR/+DM/-DM."""
            if len(vals) < p:
                return []
            sm = sum(vals[:p])       # SUM init (Wilder DI spec)
            out = [sm]
            for v in vals[p:]:
                sm = (sm * (p - 1) + v) / p   # [FIX-ADX-01] recorrência correta
                out.append(sm)
            return out

        def _wilder_mean(vals, p):
            """MEAN init + corrected recurrence — para DX→ADX."""
            if len(vals) < p:
                return []
            sm = sum(vals[:p]) / p   # MEAN init (Wilder ADX spec)
            out = [sm]
            for v in vals[p:]:
                sm = (sm * (p - 1) + v) / p   # [FIX-ADX-01] recorrência correta
                out.append(sm)
            return out

        tr_s = _wilder_sum(trs, period)
        p_s  = _wilder_sum(plus_dm, period)
        m_s  = _wilder_sum(minus_dm, period)
        k2   = min(len(tr_s), len(p_s), len(m_s))
        if k2 == 0:
            return None
        tr_s, p_s, m_s = tr_s[-k2:], p_s[-k2:], m_s[-k2:]

        dx = []
        last_pdi, last_mdi = 0.0, 0.0
        for trv, pv, mv in zip(tr_s, p_s, m_s):
            if trv <= 1e-12:
                dx.append(0.0)
                continue
            pdi = 100.0 * pv / trv
            mdi = 100.0 * mv / trv
            last_pdi, last_mdi = pdi, mdi
            denom = pdi + mdi
            dx.append(0.0 if denom <= 1e-12 else 100.0 * abs(pdi - mdi) / denom)

        adx_s = _wilder_mean(dx, period)   # [FIX-ADX-02] MEAN init for ADX
        if not adx_s:
            return None

        result = adx_s[-1]

        # [FIX-ADX-03] Clamp defensivo: ADX deve ser 0–100. Valor fora do range
        # indica dado inválido — clampamos e logamos para diagnóstico.
        if result > 100.0:
            logger.warning(
                "ADX CLAMP: valor raw=%.2f > 100 (possível dado inválido) → forçado 100.0 | "
                "+DI=%.2f -DI=%.2f dx_last=%.2f period=%d n=%d",
                result, last_pdi, last_mdi, dx[-1] if dx else 0, period, n,
            )
            result = 100.0

        # [FIX-ADX-04] Log debug para diagnóstico (visível com log level DEBUG)
        logger.debug(
            "ADX calc: +DI=%.2f -DI=%.2f DX=%.2f ADX=%.2f (period=%d n=%d)",
            last_pdi, last_mdi, dx[-1] if dx else 0, result, period, n,
        )

        return result


# ══════════════════════════════════════════════════════════════════════════════
# BOT
# ══════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════
# [EXIT-CLASSIFY-01] Classifica saída como TP, SL ou MANUAL/UNKNOWN
# Usa tolerância EPS=0.1% para absorver spread/slippage no fill.
# LONG:  exit >= tp*(1-EPS) → TP  |  exit <= sl*(1+EPS) → SL
# SHORT: exit <= tp*(1+EPS) → TP  |  exit >= sl*(1-EPS) → SL
# ═══════════════════════════════════════════════════════════════
_EXIT_EPS = 0.001  # 0.1% de tolerância

def classify_exit(side: str, exit_px: float, tp_px: float, sl_px: float) -> str:
    """Retorna 'TP', 'SL' ou 'MANUAL/UNKNOWN'."""
    if not exit_px or not tp_px or not sl_px:
        return "MANUAL/UNKNOWN"
    if side.upper() in ("LONG", "BUY"):
        if exit_px >= tp_px * (1 - _EXIT_EPS):
            return "TP"
        if exit_px <= sl_px * (1 + _EXIT_EPS):
            return "SL"
    else:  # SHORT / SELL
        if exit_px <= tp_px * (1 + _EXIT_EPS):
            return "TP"
        if exit_px >= sl_px * (1 - _EXIT_EPS):
            return "SL"
    return "MANUAL/UNKNOWN"


class Bot:
    def __init__(self, config):
        self.config             = config
        self.client             = ExchangeClient(config)
        self.initial_equity     = None
        self.state_date         = None
        self._open_inst_ids     = set()
        # Posições abertas (para gates: correlação / mesma-direção)
        self._open_positions    = {}   # instId -> pos (float)
        self._open_long_count   = 0
        self._open_short_count  = 0

        # Volatility Guard (BTC) — quando ativo, bloqueia novas entradas até esse timestamp (epoch seconds)
        self._vol_guard_until      = 0.0
        self._vol_guard_last_check = 0.0
        # Cache de candles BTC para o volatility guard (evita busca extra por ticker)
        self._btc_candles_cache: list = []
        self._btc_cache_ts: float     = 0.0

        # [ENV-06] Cooldown por símbolo: ts da última ordem enviada
        self._symbol_last_trade: dict = {}
        # Contador sequencial de trades nesta sessão
        self._trade_seq = 0
        
        # ── Protections (Stoploss cluster / Drawdown window / Pair lock) ────────
        self._sl_events = deque()          # timestamps (epoch seconds) de fechamentos por SL
        self._dd_events = deque()          # (ts, pnl_usd) de trades fechados
        self._pair_perf = {}               # instId -> deque[(ts, pnl_pct)]
        self._pair_lock_until = {}         # instId -> epoch seconds
        self._sl_guard_until = 0.0
        self._dd_guard_until = 0.0
        self._last_guard_log = 0.0

# [EXIT-01] Rastreamento de posições abertas para detectar fechamentos por TP/SL
        # dict: instId -> {trade_id, entry_px, tp_px, sl_px, signal, ts, algo_id, be_moved}
        self._tracked_positions: dict = {}

        # [FIX-CB] Circuit breaker persistente — bloqueia entradas até virar o dia UTC
        # (não apenas dorme 1h). Flag setado em True e resetado em _check_day_reset().\
        self._cb_halted: bool = False

        # [PUMP-05] Conjunto de instIds detectados como pump no ciclo atual.
        # Atualizado por _pump_scan() a cada ciclo, consumido por _get_signal().
        self._pump_set: set = set()

        # ── Dual Mode runtime (v3.1.0) ──────────────────────────────────────────
        # [FIX-DM-INIT] Inicializados aqui (não em _check_day_reset).
        # Defaults = modo DAY até _update_dual_mode() avaliar o regime.
        self._dual_mode:          str   = "DAY"
        self._dual_confirm:       int   = 0
        self._dual_last_mode:     str   = "DAY"
        self._dual_last_check_ts: float = 0.0

        # Parâmetros efetivos — trocam a cada modo; usados em vez de config.* no loop
        self._eff_min_rr          = config.DAY_MIN_RR           # ex.: 1.3
        self._eff_atr_mult_sl     = config.DAY_ATR_MULT_SL      # ex.: 1.2
        self._eff_sl_max_pct      = config.DAY_SL_MAX_PCT        # ex.: 0.015
        self._eff_corr_threshold  = config.DAY_CORR_THRESHOLD    # ex.: 0.95
        self._eff_min_edge_pct    = config.MIN_EDGE_PCT          # ex.: 0.40 (% do preço)
        self._eff_cooldown_sec    = config.DAY_COOLDOWN_SEC      # ex.: 600
        self._eff_max_positions   = config.MAX_POSITIONS
        self._eff_max_same_dir    = config.MAX_SAME_DIRECTION

    # ── Estado persistente ────────────────────────────────────────────────────
    def _load_state(self):
        today = today_utc_str()
        try:
            with open(self.config.STATE_PATH, "r", encoding="utf-8") as f:
                state = json.load(f)
            if state.get("date") == today:
                self.initial_equity = float(state["initial_equity"])
                self.state_date     = today
                logger.info("Estado carregado: equity=%.2f (dia %s)", self.initial_equity, today)
                return
        except Exception:
            pass

        logger.info("Novo dia UTC (%s) — equity será definida no primeiro ciclo", today)
        self.state_date     = today
        self.initial_equity = None

    def _save_state(self, equity):
        try:
            ensure_parent_dir(self.config.STATE_PATH)
            with open(self.config.STATE_PATH, "w", encoding="utf-8") as f:
                json.dump({"date": self.state_date, "initial_equity": float(equity)}, f)
        except Exception as exc:
            logger.warning("Falha ao salvar estado: %s", exc)

    def _check_day_reset(self):
        today = today_utc_str()
        if self.state_date and self.state_date != today:
            logger.info("Novo dia UTC (%s) — resetando circuit breaker, guards e buffers de proteção", today)
            self.state_date     = today
            self.initial_equity = None
            self._cb_halted     = False   # [FIX-CB] libera entradas no novo dia
            # [FIX-P1-RESET] Reseta todos os buffers de proteção intra-dia
            self._sl_guard_until = 0.0
            self._dd_guard_until = 0.0
            self._sl_events.clear()
            self._dd_events.clear()
            self._pair_lock_until.clear()
            logger.info("Proteções resetadas: SL guard, DD guard, Pair locks, SL/DD events.")

    # ── Saldo ─────────────────────────────────────────────────────────────────
    async def _get_equity(self):
        data = await self.client.get_balance()
        if not data:
            return None
        try:
            # Tenta totalEq primeiro (valor total da conta em USDT)
            total = float(data[0].get("totalEq", 0) or 0)
            if total > 0:
                return total
            # Fallback: soma details USDT
            for d in data[0].get("details", []):
                if d.get("ccy") == "USDT":
                    return float(d.get("eq", 0) or 0)
        except Exception as exc:
            logger.error("Erro ao parsear saldo: %s", exc)
        return None

    async def _get_avail(self):
        data = await self.client.get_balance()
        if not data:
            return 0.0
        try:
            for d in data[0].get("details", []):
                if d.get("ccy") == "USDT":
                    return float(d.get("availBal", 0) or 0)
        except Exception:
            pass
        return 0.0

    # ── Posições ──────────────────────────────────────────────────────────────
    async def _refresh_positions(self):
        self._open_inst_ids.clear()
        self._open_positions.clear()
        self._open_long_count = 0
        self._open_short_count = 0

        data = await self.client.get_positions()
        if not data:
            return 0

        count = 0
        for p in data:
            inst = p.get("instId", "")
            pos  = float(p.get("pos", 0) or 0)
            if inst and abs(pos) > 0:
                self._open_inst_ids.add(inst)
                self._open_positions[inst] = pos
                if pos > 0:
                    self._open_long_count += 1
                else:
                    self._open_short_count += 1
                count += 1
        return count

    # ── Universo de ativos ────────────────────────────────────────────────────
    async def _build_universe(self, tickers=None):
        """
        Constrói o universo de ativos elegíveis.
        [OPT-TICKERS] Aceita tickers já buscados para evitar chamada dupla à API.
        Se não passado, busca internamente (compatibilidade retroativa).
        [PUMP-02] Quando PUMP_ENABLED=true:
          - PUMP_UNIVERSE_ONLY=true  → universo é APENAS os ativos em pump.
          - PUMP_UNIVERSE_ONLY=false → pumps são inseridos no TOPO da lista,
            seguidos pelos demais ativos por volume (prioridade sem exclusão).
        """
        if tickers is None:
            tickers = await self.client.get_tickers()
        if not tickers:
            return []

        eligible = []
        for t in tickers:
            inst = t.get("instId", "")

            # Só USDT perpetuals
            if "-USDT-SWAP" not in inst:
                continue

            # Filtro de volume mínimo
            try:
                vol_usdt = float(t.get("volCcy24h", 0) or 0)
            except (ValueError, TypeError):
                continue

            if vol_usdt < self.config.MIN_VOLUME_24H_USDT:
                continue

            eligible.append(t)

        # Ordena por volume
        eligible.sort(key=lambda x: float(x.get("volCcy24h", 0) or 0), reverse=True)

        # [PUMP-02] Filtragem/priorização por pump
        if self.config.PUMP_ENABLED and self._pump_set:
            pumps   = [t for t in eligible if t.get("instId", "") in self._pump_set]
            others  = [t for t in eligible if t.get("instId", "") not in self._pump_set]
            if self.config.PUMP_UNIVERSE_ONLY:
                eligible = pumps          # só pumps
            else:
                eligible = pumps + others  # pumps primeiro, resto na sequência

        result = eligible[:self.config.MAX_UNIVERSE_SIZE]
        pump_count = sum(1 for t in result if t.get("instId", "") in self._pump_set)
        logger.info(
            "Universo: %d ativos (volume > %.0fM USDT)%s",
            len(result),
            self.config.MIN_VOLUME_24H_USDT / 1_000_000,
            f" | 🚀 {pump_count} pumps priorizados" if pump_count else "",
        )
        return result

    # ── Geração de sinal ──────────────────────────────────────────────────────
    async def _get_signal(self, instId):
        # Candles 5m
        c5 = await self.client.get_candles(instId, self.config.BAR_INTERVAL, limit="120")
        if not c5 or len(c5) < 30:
            return None, None

        # OKX retorna candles do mais recente para o mais antigo → inverter
        c5 = list(reversed(c5))

        o = float(c5[-1][1])
        h = float(c5[-1][2])
        l = float(c5[-1][3])
        c = float(c5[-1][4])
        v = float(c5[-1][5])

        closes = [float(x[4]) for x in c5]
        vols   = [float(x[5]) for x in c5]

        # Bollinger Bands + RSI
        _, upper, lower = Indicators.bb(closes, self.config.BB_PERIOD, self.config.BB_STD)
        rsi_vals         = Indicators.rsi(closes, self.config.RSI_LEN)

        if upper is None or not rsi_vals:
            return None, None

        rsi_val  = rsi_vals[-1]
        avg_vol  = sum(vols[-20:-1]) / 19 if len(vols) >= 20 else sum(vols) / len(vols)
        is_climax = v >= avg_vol * self.config.VOLUME_CLIMAX_MULT

        rng        = max(h - l, 1e-12)
        pavio_sup  = h - max(o, c)
        pavio_inf  = min(o, c) - l

        # Sinal base BB + RSI + Volume + Pavio
        signal = None
        if c > upper and rsi_val > self.config.RSI_OB and is_climax:
            if (pavio_sup / rng) > self.config.PAVIO_PCT_MIN:
                signal = "sell"
        elif c < lower and rsi_val < self.config.RSI_OS and is_climax:
            if (pavio_inf / rng) > self.config.PAVIO_PCT_MIN:
                signal = "buy"

        if signal is None:
            return None, None

        # [PUMP-03] Filtro direcional pump-aware
        # Lógica: pumps são candidatos a reversão (exaustão) → só SHORT.
        # BUY em pump = entrar em ativo sobrecomprado → risco alto.
        # SELL sem pump = shortar ativo sem catalisador → menos confiável.
        if self.config.PUMP_ENABLED:
            is_pump = instId in self._pump_set
            if signal == "buy" and is_pump and not self.config.PUMP_ALLOW_LONGS:
                logger.info("[%s] ⛔ PumpWatch: BUY bloqueado — ativo em pump (+20%% 24h)", instId)
                return None, None
            if signal == "sell" and not is_pump and self.config.PUMP_REQUIRE_PUMP_FOR_SELL:
                logger.debug("[%s] PumpWatch: SELL ignorado — ativo sem pump detectado", instId)
                return None, None

        logger.info(
            "[%s] Sinal base: %s | RSI=%.1f | Vol=%.0f/%.0f%s",
            instId, signal.upper(), rsi_val, v, avg_vol,
            " 🚀pump" if instId in self._pump_set else "",
        )

        # ── Filtros 15m (quando exigidos pelo perfil) ─────────────────────────
        if self.config.REQUIRE_DOUBLE_PATTERN or self.config.REQUIRE_RSI_DIVERGENCE or self.config.REQUIRE_VOLUME_DECAY:
            c15 = await self.client.get_candles(instId, self.config.PATTERN_INTERVAL, limit="60")
            if not c15 or len(c15) < 20:
                return None, None

            c15      = list(reversed(c15))
            closes15 = [float(x[4]) for x in c15]
            highs15  = [float(x[2]) for x in c15]
            lows15   = [float(x[3]) for x in c15]
            vols15   = [float(x[5]) for x in c15]
            rsi15    = Indicators.rsi(closes15, self.config.RSI_LEN)

            # Topo/Fundo duplo
            if self.config.REQUIRE_DOUBLE_PATTERN:
                if signal == "sell":
                    ok = Indicators.find_double_top(
                        highs15, closes15,
                        self.config.DOUBLE_TOP_TOLERANCE_PCT,
                        self.config.DOUBLE_TOP_MIN_CANDLES,
                        self.config.DOUBLE_TOP_MAX_CANDLES,
                    )
                    if not ok:
                        logger.info("[%s] Rejeitado: sem topo duplo", instId)
                        return None, None
                else:
                    ok = Indicators.find_double_bottom(
                        lows15, closes15,
                        self.config.DOUBLE_TOP_TOLERANCE_PCT,
                        self.config.DOUBLE_TOP_MIN_CANDLES,
                        self.config.DOUBLE_TOP_MAX_CANDLES,
                    )
                    if not ok:
                        logger.info("[%s] Rejeitado: sem fundo duplo", instId)
                        return None, None

            # Divergência RSI
            if self.config.REQUIRE_RSI_DIVERGENCE and rsi15:
                direction = "bearish" if signal == "sell" else "bullish"
                if not Indicators.check_rsi_divergence(
                    closes15, rsi15, direction, self.config.RSI_DIVERGENCE_MIN
                ):
                    logger.info("[%s] Rejeitado: sem divergência RSI %s", instId, direction)
                    return None, None

            # Volume decrescente
            if self.config.REQUIRE_VOLUME_DECAY:
                if not Indicators.check_volume_decay(vols15, self.config.VOLUME_DECAY_THRESHOLD):
                    logger.info("[%s] Rejeitado: volume não decrescente", instId)
                    return None, None

        logger.info("[%s] ✅ Todos os filtros OK — %s confirmado", instId, signal.upper())
        meta = {
            "rsi_val":    round(rsi_val, 2),
            "volume_mult": round(v / avg_vol, 2) if avg_vol else 1.0,
            "c5":         c5,   # [FIX-P1-CAND] reutilizado em _regime_filters_ok
        }
        return signal, meta

    # ── Volatility Guard Global (BTC ATR%) ───────────────────────────────────
    async def _btc_volatility_guard(self) -> tuple[bool, str]:
        """
        [FIX-P0-VOL] Verifica se o mercado está em alta volatilidade via ATR% do BTC.
        Retorna (block: bool, reason: str).

        Lógica:
        - Se o guard já está ativo (dentro do pause timer), bloqueia imediatamente.
        - Busca candles BTC-USDT-SWAP no intervalo BTC_VOL_GUARD_BAR.
        - Calcula ATR(BTC_VOL_GUARD_LOOKBACK) e divide pelo preço atual → ATR%.
        - Se ATR% > BTC_VOL_GUARD_ATR_PCT_MAX, ativa pausa por VOL_GUARD_PAUSE_SEC.
        """
        now = _wall_time.time()

        # 1. Guard ainda ativo?
        if now < self._vol_guard_until:
            remaining = int(self._vol_guard_until - now)
            return True, f"BTC ATR% guard ativo — {remaining}s restantes"

        # 2. Throttle: só re-verifica a cada 60s para não sobrecarregar a API
        if now - self._vol_guard_last_check < 60:
            return False, "ok (cached)"
        self._vol_guard_last_check = now

        try:
            lookback = max(self.config.BTC_VOL_GUARD_LOOKBACK, 30)
            candles  = await self.client.get_candles(
                "BTC-USDT-SWAP",
                self.config.BTC_VOL_GUARD_BAR,
                limit=str(lookback + 5),
            )
            if not candles or len(candles) < lookback:
                logger.debug("Volatility Guard: candles BTC insuficientes — ignorando gate")
                return False, "dados insuficientes"

            candles = list(reversed(candles))  # ordem cronológica

            atr_val = calc_atr(candles, period=14)
            if not atr_val:
                return False, "ATR indisponível"

            btc_px = float(candles[-1][4])   # último close
            if btc_px <= 0:
                return False, "preço BTC inválido"

            atr_pct = atr_val / btc_px       # ATR como fração do preço

            logger.info(
                "🔍 Volatility Guard: BTC ATR=%.2f ATR%%=%.3f%% threshold=%.3f%%",
                atr_val, atr_pct * 100, self.config.BTC_VOL_GUARD_ATR_PCT_MAX * 100,
            )

            if atr_pct > self.config.BTC_VOL_GUARD_ATR_PCT_MAX:
                self._vol_guard_until = now + self.config.BTC_VOL_GUARD_PAUSE_SEC
                reason = (
                    f"BTC ATR%={atr_pct*100:.3f}% > max={self.config.BTC_VOL_GUARD_ATR_PCT_MAX*100:.3f}% "
                    f"— pausa de {self.config.BTC_VOL_GUARD_PAUSE_SEC}s ativada"
                )
                logger.warning("⏸ Volatility Guard: %s", reason)
                log_event("volatility_guard",
                          btc_atr=round(atr_val, 6),
                          btc_atr_pct=round(atr_pct * 100, 4),
                          threshold_pct=round(self.config.BTC_VOL_GUARD_ATR_PCT_MAX * 100, 4),
                          pause_sec=self.config.BTC_VOL_GUARD_PAUSE_SEC)
                return True, reason

            return False, f"ok — ATR%={atr_pct*100:.3f}%"

        except Exception as exc:
            logger.debug("Volatility Guard: erro (%s) — ignorando gate", exc)
            return False, f"erro: {exc}"

    # ── Correlation Gate ──────────────────────────────────────────────────────
    async def _correlation_gate(self, instId: str, signal: str) -> tuple[bool, str]:
        """
        [OPT-CORR] Bloqueia entrada se a correlação EFETIVA com posições abertas
        exceder o threshold. Correlação efetiva leva em conta a direção:

            eff_corr = r × sign_open × sign_candidate
              onde sign = +1 (long/buy) ou -1 (short/sell)

        Isso significa:
          - Candidato BUY altamente correlacionado com posição LONG aberta → bloqueia
            (acúmulo de risco na mesma direção)
          - Candidato SELL altamente correlacionado com posição LONG aberta → NÃO bloqueia
            (hedge — direções opostas, risco se cancela)

        Retorna (block: bool, reason: str).
        """
        # Sem posições abertas → não há nada para correlacionar
        if not self._open_positions:
            return False, "sem posições abertas"

        lookback    = max(self.config.CORR_LOOKBACK, 20)
        min_periods = min(30, lookback)
        # Sinal do candidato: +1 = buy/long, -1 = sell/short
        cand_sign   = 1 if signal == "buy" else -1

        try:
            cands = await self.client.get_candles(
                instId, self.config.CORR_BAR, limit=str(lookback + 5)
            )
            if not cands or len(cands) < lookback:
                return False, "dados insuficientes (candidato)"

            cands    = list(reversed(cands))
            closes_c = [float(c[4]) for c in cands[-lookback:]]
            if any(p <= 0 for p in closes_c):
                return False, "preço inválido (candidato)"

            # Retornos log do candidato, alinhados por índice de posição na série
            ret_c = [
                math.log(closes_c[i] / closes_c[i - 1])
                for i in range(1, len(closes_c))
            ]

        except Exception as exc:
            logger.debug("[%s] Correlation Gate: erro ao buscar candles: %s", instId, exc)
            return False, f"erro candles candidato: {exc}"

        # Correlaciona com cada posição aberta
        worst = None   # rastreia o pior caso (maior eff_corr)
        for open_inst, pos in list(self._open_positions.items()):
            if open_inst == instId:
                continue
            try:
                ocandles = await self.client.get_candles(
                    open_inst, self.config.CORR_BAR, limit=str(lookback + 5)
                )
                if not ocandles or len(ocandles) < lookback:
                    continue

                ocandles  = list(reversed(ocandles))
                closes_o  = [float(c[4]) for c in ocandles[-lookback:]]
                if any(p <= 0 for p in closes_o):
                    continue

                ret_o = [
                    math.log(closes_o[i] / closes_o[i - 1])
                    for i in range(1, len(closes_o))
                ]

                # Pearson sobre retornos alinhados pelo mínimo de comprimento
                n = min(len(ret_c), len(ret_o))
                if n < min_periods:
                    continue

                rc = ret_c[-n:]
                ro = ret_o[-n:]

                mean_c = sum(rc) / n
                mean_o = sum(ro) / n
                num    = sum((rc[i] - mean_c) * (ro[i] - mean_o) for i in range(n))
                den_c  = math.sqrt(sum((x - mean_c) ** 2 for x in rc))
                den_o  = math.sqrt(sum((x - mean_o) ** 2 for x in ro))

                if den_c == 0 or den_o == 0:
                    continue

                r = max(-1.0, min(1.0, num / (den_c * den_o)))

                # [OPT-CORR] Correlação efetiva: considera direção de ambas as posições.
                # sign_open = +1 se long (pos > 0), -1 se short (pos < 0)
                open_sign = 1 if pos > 0 else -1
                eff_corr  = r * open_sign * cand_sign

                logger.debug(
                    "[%s] Correlation Gate vs %s: r=%.3f eff=%.3f (open=%s cand=%s threshold=%.2f)",
                    instId, open_inst, r, eff_corr,
                    "long" if open_sign > 0 else "short",
                    "buy" if cand_sign > 0 else "sell",
                    self._eff_corr_threshold,
                )

                if worst is None or eff_corr > worst["eff"]:
                    worst = {"eff": eff_corr, "r": r, "inst": open_inst, "n": n}

            except Exception as exc:
                logger.debug("[%s] Correlation Gate: erro vs %s: %s", instId, open_inst, exc)
                continue

        if worst and worst["eff"] >= self._eff_corr_threshold:
            reason = (
                f"eff_corr={worst['eff']:.3f} (r={worst['r']:.3f}) "
                f"vs {worst['inst']} n={worst['n']} bar={self.config.CORR_BAR}"
            )
            log_event("correlation_gate",
                      symbol=instId, signal=signal.upper(),
                      corr_with=worst["inst"],
                      eff_corr=round(worst["eff"], 4),
                      raw_corr=round(worst["r"], 4),
                      threshold=self._eff_corr_threshold,
                      bar=self.config.CORR_BAR, lookback=lookback)
            return True, reason

        return False, "ok"

    # ── Loop principal ────────────────────────────────────────────────────────
    
    def _now(self) -> float:
        return _wall_time.time()

    def _pair_locked(self, instId: str) -> tuple[bool, str]:
        until = self._pair_lock_until.get(instId, 0.0)
        if until and self._now() < until:
            return True, f"pair_lock ativo ({int(until - self._now())}s)"
        return False, ""

    def _global_guard_block(self) -> tuple[bool, str]:
        now = self._now()
        if self._sl_guard_until and now < self._sl_guard_until:
            return True, f"stoploss_guard ativo ({int(self._sl_guard_until - now)}s)"
        if self._dd_guard_until and now < self._dd_guard_until:
            return True, f"drawdown_guard ativo ({int(self._dd_guard_until - now)}s)"
        return False, ""

    def _record_close_for_protections(self, instId: str, exit_reason: str, pnl_pct: float, trade_margin: float | None):
        """Atualiza buffers para protections após um trade fechar."""
        now = self._now()

        # Pair perf
        dq = self._pair_perf.setdefault(instId, deque(maxlen=20))
        dq.append((now, float(pnl_pct)))

        # Stoploss events
        if exit_reason == "SL":
            self._sl_events.append(now)

        # Drawdown window em USDT (aprox): margin * pnl%
        if trade_margin is None:
            trade_margin = float(self.config.NOTIONAL_USD or 0.0)
        pnl_usd = float(trade_margin) * float(pnl_pct) / 100.0
        self._dd_events.append((now, pnl_usd))

        # Aplica pair lock (se desempenho recente ruim)
        if len(dq) >= self.config.PAIR_LOCK_MIN_TRADES:
            last = list(dq)[-self.config.PAIR_LOCK_MIN_TRADES:]
            avg = sum(p for _, p in last) / max(len(last), 1)
            if avg <= self.config.PAIR_LOCK_MAX_AVG_PNL_PCT:
                self._pair_lock_until[instId] = now + self.config.PAIR_LOCK_SEC
                logger.warning("[%s] 🔒 PairLock: avg_pnl=%.2f%% em %d trades — lock %ds",
                               instId, avg, len(last), self.config.PAIR_LOCK_SEC)
                log_event("protection", symbol=instId, kind="pair_lock",
                          avg_pnl_pct=avg, window_trades=len(last), lock_sec=self.config.PAIR_LOCK_SEC)

        # Stoploss guard window
        win = self.config.SL_GUARD_WINDOW_MIN * 60
        while self._sl_events and (now - self._sl_events[0]) > win:
            self._sl_events.popleft()
        if len(self._sl_events) >= self.config.SL_GUARD_MAX_SL:
            self._sl_guard_until = max(self._sl_guard_until, now + self.config.SL_GUARD_PAUSE_SEC)
            logger.warning("⛔ StoplossGuard: %d SL em %dmin — pausa entradas por %ds",
                           len(self._sl_events), self.config.SL_GUARD_WINDOW_MIN, self.config.SL_GUARD_PAUSE_SEC)
            log_event("protection", kind="stoploss_guard", sl_count=len(self._sl_events),
                      window_min=self.config.SL_GUARD_WINDOW_MIN, pause_sec=self.config.SL_GUARD_PAUSE_SEC)

        # Drawdown guard window (últimos N fechamentos)
        # Mantém apenas os últimos N trades
        while len(self._dd_events) > max(self.config.DD_GUARD_WINDOW_TRADES, 1):
            self._dd_events.popleft()
        if self.initial_equity:
            recent_pnl = sum(p for _, p in self._dd_events)
            dd_pct = -recent_pnl / max(self.initial_equity, 1e-12)  # perda -> positivo
            if dd_pct >= self.config.DD_GUARD_PCT:
                self._dd_guard_until = max(self._dd_guard_until, now + self.config.DD_GUARD_PAUSE_SEC)
                logger.warning("⛔ DrawdownGuard: dd=%.2f%% em %d trades — pausa entradas por %ds",
                               dd_pct*100, len(self._dd_events), self.config.DD_GUARD_PAUSE_SEC)
                log_event("protection", kind="drawdown_guard", dd_pct=dd_pct,
                          window_trades=len(self._dd_events), pause_sec=self.config.DD_GUARD_PAUSE_SEC)

    async def _trend_filter_ok(self, instId: str, signal: str) -> tuple[bool, str]:
        # [FIX-DM-TOL] tol corretamente definido (sem auto-referência):
        # DUAL_MODE_ENABLED=True: usa o overlay do modo atual (DAY=0.003, POSITION=0.0)
        # DUAL_MODE_ENABLED=False: sem tolerância (comportamento legado v3.0.9)
        if self.config.DUAL_MODE_ENABLED:
            tol = (self.config.POS_TREND_TOL_PCT
                   if self._dual_mode == "POSITION"
                   else self.config.DAY_TREND_TOL_PCT)
        else:
            tol = 0.0
        if not self.config.TREND_FILTER_ENABLED:
            return True, ""
        try:
            c = await self.client.get_candles(instId, self.config.TREND_HTF_BAR, limit=str(self.config.TREND_EMA_LEN + 30))
            if not c or len(c) < self.config.TREND_EMA_LEN:
                return True, ""  # não bloqueia por falta de dados
            c = list(reversed(c))
            closes = [float(x[4]) for x in c]
            ema = Indicators.ema(closes, self.config.TREND_EMA_LEN)
            if ema is None:
                return True, ""
            last_close = closes[-1]
            # Para mean-reversion: BUY acima da EMA (bull) e SELL abaixo (bear)
            # tol cria buffer: no DAY (0.3%) permite entrar levemente abaixo/acima
            if signal.lower() == "buy" and last_close < ema * (1 - tol):
                return False, f"HTF trend DOWN (close<{self.config.TREND_EMA_LEN}EMA tol={tol*100:.1f}%)"
            if signal.lower() == "sell" and last_close > ema * (1 + tol):
                return False, f"HTF trend UP (close>{self.config.TREND_EMA_LEN}EMA tol={tol*100:.1f}%)"
            return True, ""
        except Exception as e:
            return True, f"trend_filter_error: {e}"

    async def _regime_filters_ok(self, instId: str) -> tuple[bool, str, float | None]:
        """ADX/ATR% filters usando candles 5m. Retorna (ok, reason, atr_pct)."""
        if not (self.config.ADX_FILTER_ENABLED or self.config.ATR_PCT_FILTER_ENABLED or self.config.RISK_PARITY_ENABLED):
            return True, "", None
        c5 = await self.client.get_candles(instId, self.config.BAR_INTERVAL, limit="120")
        if not c5 or len(c5) < 30:
            return True, "", None
        c5 = list(reversed(c5))
        highs = [float(x[2]) for x in c5]
        lows  = [float(x[3]) for x in c5]
        closes= [float(x[4]) for x in c5]
        last_close = closes[-1]
        atr = calc_atr(c5, period=14)
        atr_pct = (atr / last_close) if (atr and last_close) else None

        if self.config.ATR_PCT_FILTER_ENABLED and atr_pct is not None:
            if atr_pct < self.config.ATR_PCT_MIN:
                return False, f"ATR% baixo ({atr_pct*100:.2f}%)", atr_pct
            if atr_pct > self.config.ATR_PCT_MAX:
                return False, f"ATR% alto ({atr_pct*100:.2f}%)", atr_pct

        if self.config.ADX_FILTER_ENABLED and self._dual_mode != "POSITION":
            # [FIX-DM-ADX] Em POSITION mode, ADX alto é desejado (confirma tendência).
            # Apenas em DAY (mean-reversion) bloqueamos quando ADX indica forte trend.
            # [FIX-ADX-04] Log ADX value sempre (INFO quando bloqueia, DEBUG quando passa)
            adx = Indicators.adx(highs, lows, closes, period=self.config.ADX_LEN)
            if adx is not None:
                if adx > self.config.ADX_MAX_FOR_MEANREV:
                    logger.info(
                        "[%s] RegimeFilter: ADX=%.1f > %.1f (max mean-rev) — modo DAY → BLOQUEADO",
                        instId, adx, self.config.ADX_MAX_FOR_MEANREV,
                    )
                    return False, f"ADX alto — modo DAY ({adx:.1f})", atr_pct
                else:
                    logger.debug(
                        "[%s] RegimeFilter: ADX=%.1f <= %.1f → OK",
                        instId, adx, self.config.ADX_MAX_FOR_MEANREV,
                    )

        return True, "", atr_pct

    def _risk_parity_margin(self, base_margin: float, atr_pct: float | None) -> float:
        if not self.config.RISK_PARITY_ENABLED or atr_pct is None or atr_pct <= 1e-9:
            return base_margin
        target = self.config.RISK_PARITY_TARGET_ATR_PCT
        factor = target / atr_pct
        factor = max(self.config.RISK_PARITY_MIN_FACTOR, min(self.config.RISK_PARITY_MAX_FACTOR, factor))
        return float(base_margin) * float(factor)

    # ────────────────────────────────────────────────────────────────────────────
    # Dual Mode helpers (v3.1.0)
    # ────────────────────────────────────────────────────────────────────────────

    async def _detect_regime(self) -> tuple[str, str]:
        """
        Detecta regime global (DAY vs POSITION) usando REGIME_SYMBOL (default BTC 1H).
        Retorna (mode: str, reason: str).

        Lógica:
          - Busca REGIME_EMA_LEN+30 candles no REGIME_BAR do REGIME_SYMBOL.
          - Calcula EMA(REGIME_EMA_LEN) e ADX(REGIME_ADX_PERIOD).
          - ADX >= REGIME_ADX_TREND_MIN → POSITION (mercado trendindo)
          - ADX <  REGIME_ADX_TREND_MIN → DAY (chop / mean-reversion)
          - Fallback em qualquer erro: DAY (conservador)
        """
        if not self.config.DUAL_MODE_ENABLED:
            return "DAY", "dual_mode_disabled"
        try:
            inst = self.config.REGIME_SYMBOL
            bar  = self.config.REGIME_BAR
            need = max(self.config.REGIME_EMA_LEN + 30,
                       self.config.REGIME_ADX_PERIOD * 3 + 5)
            c = await self.client.get_candles(inst, bar, limit=str(min(500, need)))
            if not c or len(c) < (self.config.REGIME_EMA_LEN + 5):
                return "DAY", "insufficient_regime_candles"
            c      = list(reversed(c))
            closes = [float(x[4]) for x in c]
            highs  = [float(x[2]) for x in c]
            lows   = [float(x[3]) for x in c]

            ema = Indicators.ema(closes, self.config.REGIME_EMA_LEN)
            adx = Indicators.adx(highs, lows, closes,
                                 period=self.config.REGIME_ADX_PERIOD)
            if ema is None or adx is None:
                return "DAY", "regime_indicators_unavailable"

            last_close = closes[-1]
            is_trend   = adx >= float(self.config.REGIME_ADX_TREND_MIN)

            if is_trend:
                return (
                    "POSITION",
                    f"trend: ADX={adx:.1f}>={self.config.REGIME_ADX_TREND_MIN:.1f} "
                    f"close={last_close:.6g} ema={ema:.6g}"
                )
            return "DAY", f"chop: ADX={adx:.1f}<{self.config.REGIME_ADX_TREND_MIN:.1f}"

        except Exception as e:
            return "DAY", f"regime_error: {e}"

    def _apply_mode_overlays(self, mode: str) -> None:
        """
        Aplica overlays de parâmetros para o modo especificado.
        [FIX-DM-RESTORE] DAY branch restaura explicitamente max_positions/max_same_dir
        de config.* (antes era no-op auto-referencial).
        """
        if not self.config.DUAL_MODE_ENABLED:
            mode = "DAY"
        self._dual_mode = mode

        if mode == "POSITION":
            self._eff_min_rr          = self.config.POS_MIN_RR
            self._eff_atr_mult_sl     = self.config.POS_ATR_MULT_SL
            self._eff_sl_max_pct      = self.config.POS_SL_MAX_PCT
            self._eff_corr_threshold  = self.config.POS_CORR_THRESHOLD
            self._eff_cooldown_sec    = self.config.POS_COOLDOWN_SEC
            # POSITION é mais seletivo: limita a 2 posições simultâneas
            self._eff_max_positions   = min(self.config.MAX_POSITIONS, 2)
            self._eff_max_same_dir    = min(self.config.MAX_SAME_DIRECTION, 2)
        else:  # DAY
            self._eff_min_rr          = self.config.DAY_MIN_RR
            self._eff_atr_mult_sl     = self.config.DAY_ATR_MULT_SL
            self._eff_sl_max_pct      = self.config.DAY_SL_MAX_PCT
            self._eff_corr_threshold  = self.config.DAY_CORR_THRESHOLD
            self._eff_cooldown_sec    = self.config.DAY_COOLDOWN_SEC
            # [FIX-DM-RESTORE] Restaurar de config.* explicitamente
            self._eff_max_positions   = self.config.MAX_POSITIONS
            self._eff_max_same_dir    = self.config.MAX_SAME_DIRECTION

    async def _update_dual_mode(self) -> None:
        """
        Atualiza o modo DAY/POSITION com sistema anti-flip (confirmação por ciclos).
        Deve ser chamado no início de cada iteração do while True.

        Anti-flip: o modo só troca após REGIME_CONFIRM_CYCLES ciclos consecutivos
        apontando para o mesmo target. Evita alternâncias por ruído de curto prazo.
        """
        if not self.config.DUAL_MODE_ENABLED:
            if self._dual_mode != "DAY":
                self._apply_mode_overlays("DAY")
            return

        target, reason = await self._detect_regime()

        if target == self._dual_last_mode:
            # Mesmo alvo: incrementa contagem de confirmação (cap no máximo)
            self._dual_confirm = min(
                self._dual_confirm + 1,
                max(1, self.config.REGIME_CONFIRM_CYCLES)
            )
        else:
            # Alvo diferente: reinicia contagem e registra novo alvo
            self._dual_confirm    = 1
            self._dual_last_mode  = target

        if self._dual_confirm >= max(1, self.config.REGIME_CONFIRM_CYCLES):
            prev = self._dual_mode
            self._apply_mode_overlays(target)
            if prev != self._dual_mode:
                logger.warning(
                    "🧭 DUAL_MODE switch: %s → %s (%s) | "
                    "min_rr=%.2f atr_mult=%.1f sl_max=%.1f%% cooldown=%ds",
                    prev, self._dual_mode, reason,
                    self._eff_min_rr, self._eff_atr_mult_sl,
                    self._eff_sl_max_pct * 100, self._eff_cooldown_sec,
                )
        else:
            logger.debug(
                "🧭 DUAL_MODE pending: target=%s confirm=%d/%d (%s)",
                target, self._dual_confirm,
                max(1, self.config.REGIME_CONFIRM_CYCLES), reason,
            )

    # ────────────────────────────────────────────────────────────────────────────
    # Auto Break-Even (v3.1.2)
    # ────────────────────────────────────────────────────────────────────────────

    async def _check_break_even(self) -> None:
        """
        [FIX-BE-04] Monitora posições abertas e move o SL para break-even
        quando o PnL intra-trade atinge BE_TRIGGER_PCT.

        Fluxo BE com fallback:
          1) Tenta amend-algo (com instId correto — fix v3.1.3).
          2) Se amend falhar → fallback cancel+recreate:
               a) Cancela OCO atual via /cancel-algos.
               b) Recria OCO com SL=break_even + mesmo TP.
               c) Atualiza algo_id no _tracked_positions.
             Janela desprotegida: ~100-300ms (aceitável para day trade 5m).

        Regras de segurança:
          - Não move BE mais de uma vez por posição (flag be_moved).
          - Não piora o SL: novo SL deve ser melhor que o atual.
          - Falha silenciosa: erros não interrompem o loop principal.
        """
        if not self.config.BE_ENABLED:
            return

        for instId, data in list(self._tracked_positions.items()):
            # Já moveu BE nesta posição
            if data.get("be_moved"):
                continue

            algo_id    = data.get("algo_id", "")
            if not algo_id or algo_id == "?":
                continue  # OCO não confirmado ainda

            entry      = float(data.get("entry_px") or 0)
            signal     = data.get("signal", "BUY")      # "BUY" ou "SELL"
            cur_sl     = float(data.get("sl_px") or 0)
            cur_tp     = float(data.get("tp_px") or 0)
            if entry <= 0:
                continue

            # PnL atual baseado no mark price
            try:
                mark = await self.client.get_mark_price(instId)
            except Exception:
                continue
            if not mark or mark <= 0:
                continue

            pnl_frac = (
                (mark - entry) / entry if signal == "BUY"
                else (entry - mark) / entry
            )

            if pnl_frac < self.config.BE_TRIGGER_PCT:
                continue  # ainda não atingiu o gatilho

            # Reutiliza tick_sz salvo em _tracked_positions (evita chamada extra)
            tick_sz = data.get("tick_sz", 0.00001) or 0.00001

            # Calcula novo SL em break-even + offset
            offset = self.config.BE_OFFSET_PCT
            new_sl = (
                entry * (1.0 + offset) if signal == "BUY"
                else entry * (1.0 - offset)
            )
            new_sl = round_to_tick(new_sl, tick_sz)

            # Garante que o novo SL é melhor que o atual (não piora proteção)
            if signal == "BUY"  and new_sl <= cur_sl:
                continue
            if signal == "SELL" and new_sl >= cur_sl:
                continue

            new_sl_str = str(new_sl)
            new_tp_str = str(round_to_tick(cur_tp, tick_sz)) if cur_tp > 0 else None

            # ── Tentativa 1: amend-algo (rápido, sem janela desprotegida) ──────
            ok = await self.client.amend_algo(instId, algo_id, new_sl_str, new_tp_str)

            if not ok:
                # ── Fallback: cancel + recreate OCO ───────────────────────────
                logger.info(
                    "[%s] BE amend falhou — tentando fallback cancel+recreate | algoId=%s",
                    instId, algo_id,
                )
                sz         = data.get("sz")
                close_side = data.get("close_side")

                if not sz or not close_side:
                    logger.warning(
                        "[%s] BE fallback impossível: sz ou close_side ausente em _tracked_positions",
                        instId,
                    )
                    continue

                # Passo 1: Cancelar OCO atual
                cancelled = await self.client.cancel_algo(instId, algo_id)
                if not cancelled:
                    logger.warning(
                        "[%s] BE fallback: falha ao cancelar algoId=%s — posição sem BE",
                        instId, algo_id,
                    )
                    continue

                # Passo 2: Recriar OCO com SL atualizado (mantém TP)
                await asyncio.sleep(0.15)   # pequena pausa para evitar race condition na OKX
                new_oco = await self.client.place_oco(
                    instId, close_side, sz,
                    cur_tp if cur_tp > 0 else None,
                    new_sl,
                    tick_sz,
                )

                if new_oco:
                    new_algo_id = new_oco[0].get("algoId", "?") if isinstance(new_oco, list) else "?"
                    data["algo_id"] = new_algo_id
                    ok = True
                    logger.info(
                        "[%s] BE fallback OK: novo OCO algoId=%s | SL=%.8f TP=%.8f",
                        instId, new_algo_id, new_sl, cur_tp,
                    )
                    log_event("be_oco_recreate",
                        symbol      = instId,
                        trade_id    = data.get("trade_id"),
                        old_algo_id = algo_id,
                        new_algo_id = new_algo_id,
                        new_sl      = round(new_sl, 8),
                        tp_px       = round(cur_tp, 8),
                    )
                else:
                    logger.error(
                        "[%s] ⚠️ BE CRÍTICO: cancel OK mas recreate falhou — POSIÇÃO SEM OCO! | trade_id=%s",
                        instId, data.get("trade_id"),
                    )
                    log_event("be_oco_recreate_failed",
                        symbol   = instId,
                        trade_id = data.get("trade_id"),
                        new_sl   = round(new_sl, 8),
                    )
                    continue

            if ok:
                data["sl_px"]    = new_sl
                data["be_moved"] = True
                logger.info(
                    "[%s] 🛡️ BE MOVE: SL %.8f → %.8f (+%.2f%% offset) | pnl=%.2f%% | trade_id=%s",
                    instId, cur_sl, new_sl, offset * 100,
                    pnl_frac * 100, data.get("trade_id"),
                )
                log_event(
                    "be_move",
                    symbol   = instId,
                    trade_id = data.get("trade_id"),
                    algo_id  = data.get("algo_id"),
                    entry    = round(entry, 8),
                    old_sl   = round(cur_sl, 8),
                    new_sl   = round(new_sl, 8),
                    pnl_pct  = round(pnl_frac * 100, 3),
                    offset   = offset,
                    signal   = signal,
                )

    # ────────────────────────────────────────────────────────────────────────────
    # Pump / Short Exhaustion Module (v3.1.2)
    # ────────────────────────────────────────────────────────────────────────────

    def _pump_scan(self, tickers: list) -> set:
        """
        [PUMP-01] Varre os tickers e retorna um set com os instIds em pump extremo.

        Um ativo é considerado em pump quando:
          - variação 24h >= PUMP_MIN_24H_CHANGE_PCT  (padrão: +20%)
          - volume 24h   >= PUMP_MIN_VOL_USDT        (padrão: $20M)

        O resultado é armazenado em self._pump_set e também retornado.
        Chamado a cada ciclo antes de _build_universe().
        """
        if not self.config.PUMP_ENABLED:
            self._pump_set = set()
            return self._pump_set

        pumps: set = set()
        min_chg = self.config.PUMP_MIN_24H_CHANGE_PCT
        min_vol = self.config.PUMP_MIN_VOL_USDT

        for t in tickers:
            inst = t.get("instId", "")
            if "-USDT-SWAP" not in inst:
                continue
            try:
                last    = float(t.get("last",    0) or 0)
                open24h = float(t.get("open24h", 0) or 0)
                vol24h  = float(t.get("volCcy24h", 0) or 0)
            except (ValueError, TypeError):
                continue

            if open24h <= 0 or last <= 0 or vol24h < min_vol:
                continue

            chg_pct = (last - open24h) / open24h   # fração positiva = pump
            if chg_pct >= min_chg:
                pumps.add(inst)
                logger.debug(
                    "[PUMP] Detectado: %s +%.1f%% 24h | vol=%.1fM USDT",
                    inst, chg_pct * 100, vol24h / 1_000_000,
                )

        if pumps:
            logger.info(
                "🚀 PumpWatch: %d ativo(s) em pump (≥+%.0f%% 24h, vol≥%.0fM): %s",
                len(pumps),
                min_chg * 100,
                min_vol / 1_000_000,
                ", ".join(sorted(pumps)[:8]),   # mostra até 8 no log
            )
        self._pump_set = pumps
        return pumps

    async def run(self):
        Config.load_secrets()
        Config.apply_relax_profile()

        self._load_state()

        logger.info("═══════════════════════════════════════════════════════")
        logger.info("  DALVAX PRO v3.1.3 — Railway Deploy")
        logger.info("  run_id: %s", RUN_ID)
        logger.info("  INVERT_SIGNAL: %s (env DALVAX_INVERT_SIGNAL)", str(self.config.INVERT_SIGNAL).lower())
        # [FIX-BOOT-01] Loga valor parsed E raw para detectar espaços invisíveis no Railway
        _raw_rt  = os.environ.get("DALVAX_REAL_TRADING_ENABLED", "(não definida)")
        _raw_dry = os.environ.get("DALVAX_DRY_RUN", "(não definida)")
        _raw_rr  = os.environ.get("DALVAX_MIN_RR", "(não definida)")
        logger.info("  Perfil: %s | Simulado: %s | DryRun: %s (raw=%r) | RealTradingEnabled: %s (raw=%r)",
                    self.config.RELAX_PROFILE, self.config.SIMULATED,
                    self.config.DRY_RUN, _raw_dry,
                    self.config.REAL_TRADING_ENABLED, _raw_rt)
        logger.info("  MIN_RR: %.2f (raw=%r) | TP mode: %s | ATR mult: %.1f | SL cap: %.1f%%",
                    self._eff_min_rr, _raw_rr,
                    self.config.TP_MODE, self._eff_atr_mult_sl,
                    self._eff_sl_max_pct * 100)
        logger.info("  Leverage: %sx (%s) | Max posições: %s | Loop: %ss",
                    self.config.LEVERAGE, self.config.MARGIN_MODE,
                    self._eff_max_positions, self.config.LOOP_SECONDS)
        # [FIX-BOOT-01] LOG ACIMA inclui MIN_RR com raw value (linha removida para evitar duplicata)
        logger.info("  Fallback SL: %.1f%% | Fallback TP: %.1f%% | CB: %.0f%%",
                    self.config.STOP_LOSS_PCT * 100,
                    self.config.TAKE_PROFIT_PCT * 100,
                    self.config.MAX_DAILY_LOSS_PCT * 100)
        logger.info("  Notional/op: %s | Funding máx: %.3f%% | Funding gate: %s",
                    f"${self.config.NOTIONAL_USD:.0f}" if self.config.NOTIONAL_USD > 0 else "auto (avail-based)",
                    self.config.MAX_FUNDING_RATE * 100, self.config.FUNDING_GATE_MODE)
        logger.info("  Universo: %d ativos | Vol mín: %.0fM USDT | Cooldown: %ds",
                    self.config.MAX_UNIVERSE_SIZE, self.config.MIN_VOLUME_24H_USDT / 1e6,
                    self._eff_cooldown_sec)
        logger.info("  Semi-auto timeout: %ds | Journal: %s",
                    self.config.SEMI_AUTO_TIMEOUT_SEC, self.config.JOURNAL_PATH)
        logger.info(
            "  [BE]   enabled=%s | trigger=%.1f%% | offset=%.2f%% | fallback=cancel+recreate",
            self.config.BE_ENABLED,
            self.config.BE_TRIGGER_PCT * 100,
            self.config.BE_OFFSET_PCT * 100,
        )
        logger.info(
            "  [PUMP] enabled=%s | min_chg=+%.0f%% | min_vol=%.0fM | allow_longs=%s | pump_only=%s",
            self.config.PUMP_ENABLED,
            self.config.PUMP_MIN_24H_CHANGE_PCT * 100,
            self.config.PUMP_MIN_VOL_USDT / 1e6,
            self.config.PUMP_ALLOW_LONGS,
            self.config.PUMP_UNIVERSE_ONLY,
        )
        logger.info(
            "  [ADX]  filter=%s | max_meanrev=%.1f | [v3.1.3: recorrência Wilder corrigida]",
            self.config.ADX_FILTER_ENABLED,
            self.config.ADX_MAX_FOR_MEANREV,
        )
        logger.info("═══════════════════════════════════════════════════════")

        # [FIX-03] Aviso critico se REAL_TRADING_ENABLED=False
        if not self.config.REAL_TRADING_ENABLED:
            logger.warning(
                "\n!!! ATENCAO: REAL_TRADING_DISABLED !!!\n"
                "Ordens NAO serao enviadas a OKX!\n"
                "Defina DALVAX_REAL_TRADING_ENABLED=true no Railway para ativar o trading real.\n"
                "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
            )

        # [JOURNAL] boot_snapshot — grava 1x no start para rastrear ambiente completo
        log_event("boot_snapshot",
            version       = "v3.1.3-railway.at1",
            simulated     = self.config.SIMULATED,
            sim_header    = "1" if self.config.SIMULATED else "0",
            dry_run       = self.config.DRY_RUN,
            real_enabled  = self.config.REAL_TRADING_ENABLED,
            relax         = self.config.RELAX_PROFILE,
            leverage      = self.config.LEVERAGE,
            margin_mode   = self.config.MARGIN_MODE,
            max_positions = self._eff_max_positions,
            max_same_dir  = self._eff_max_same_dir,
            notional_usd  = self.config.NOTIONAL_USD,
            loop_sec      = self.config.LOOP_SECONDS,
            funding_limit = self.config.MAX_FUNDING_RATE,
            funding_mode  = self.config.FUNDING_GATE_MODE,
            cooldown_sec  = self._eff_cooldown_sec,
            universe_size = self.config.MAX_UNIVERSE_SIZE,
            min_vol_usdt  = self.config.MIN_VOLUME_24H_USDT,
            cb_pct        = self.config.MAX_DAILY_LOSS_PCT,
            sl_pct        = self.config.STOP_LOSS_PCT,
            tp_pct        = self.config.TAKE_PROFIT_PCT,
            tp_mode       = self.config.TP_MODE,
            atr_mult_sl   = self._eff_atr_mult_sl,
            min_rr        = self._eff_min_rr,
            sl_max_pct    = self._eff_sl_max_pct,
            journal_path  = self.config.JOURNAL_PATH,
            corr_threshold= self._eff_corr_threshold,
            corr_lookback = self.config.CORR_LOOKBACK,
            corr_bar      = self.config.CORR_BAR,
            btc_atr_max   = self.config.BTC_VOL_GUARD_ATR_PCT_MAX,
            btc_atr_bar   = self.config.BTC_VOL_GUARD_BAR,
            vol_pause_sec = self.config.BTC_VOL_GUARD_PAUSE_SEC,
        )


        # Garantir net_mode antes de operar
        mode_ok = await self.client.set_position_mode()
        if not mode_ok:
            logger.error("Position mode inválido — encerrando.")
            return

        while True:
            # ── Dual Mode update (DAY ↔ POSITION) ─────────────────────────────
            await self._update_dual_mode()
            try:
                # Reset de dia UTC
                self._check_day_reset()

                # Saldo
                equity = await self._get_equity()
                if equity is None:
                    logger.warning("Saldo indisponível — aguardando próximo ciclo")
                    await asyncio.sleep(self.config.LOOP_SECONDS)
                    continue

                if self.initial_equity is None:
                    self.initial_equity = equity
                    self._save_state(equity)
                    logger.info("Equity inicial do dia: %.2f USDT", equity)

                # [FIX-CB] Circuit breaker persistente — bloqueia até virar o dia UTC
                if not self._cb_halted:
                    loss_pct = (self.initial_equity - equity) / max(self.initial_equity, 1e-12)
                    if loss_pct >= self.config.MAX_DAILY_LOSS_PCT:
                        self._cb_halted = True
                        logger.warning(
                            "⚠️ CIRCUIT BREAKER: perda diária %.2f%% ≥ limite %.2f%% "
                            "— entradas BLOQUEADAS até virar o dia UTC.",
                            loss_pct * 100, self.config.MAX_DAILY_LOSS_PCT * 100,
                        )
                        log_event("circuit_breaker",
                                  loss_pct=round(loss_pct * 100, 3),
                                  limit_pct=round(self.config.MAX_DAILY_LOSS_PCT * 100, 3),
                                  equity=round(equity, 2),
                                  initial_equity=round(self.initial_equity, 2))

                if self._cb_halted:
                    logger.info("⚠️ CIRCUIT BREAKER ativo — aguardando próximo dia UTC (sem entradas)")
                    await asyncio.sleep(self.config.LOOP_SECONDS)
                    continue

                # Atualizar posições abertas
                pos_count = await self._refresh_positions()
                loss_pct = (self.initial_equity - equity) / max(self.initial_equity, 1e-12) if self.initial_equity else 0.0
                logger.info(
                    "Monitor: Equity=%.2f | Posições=%d/%d | Perda=%.2f%% | CB=%s",
                    equity, pos_count, self._eff_max_positions, loss_pct * 100,
                    "HALTED" if self._cb_halted else "ok",
                )

                # ── [EXIT-03] Watcher de fechamento: detecta TP/SL executados na OKX ──
                # Compara posições rastreadas com as que ainda existem na OKX.
                # Se um instId saiu de _open_inst_ids mas estava em _tracked_positions,
                # a OKX fechou a posição (TP, SL ou manual).
                closed_insts = [
                    inst for inst in list(self._tracked_positions.keys())
                    if inst not in self._open_inst_ids
                ]
                for inst in closed_insts:
                    data = self._tracked_positions.pop(inst, None)
                    if not data:
                        continue
                    try:
                        mark = await self.client.get_mark_price(inst)
                        exit_px = float(mark) if mark else 0.0
                    except Exception:
                        exit_px = 0.0
                    entry_px  = float(data.get("entry_px") or 0)
                    tp_px     = float(data.get("tp_px") or 0)
                    sl_px     = float(data.get("sl_px") or 0)
                    side_exit = "LONG" if data.get("signal", "BUY") == "BUY" else "SHORT"
                    # [EXIT-CLASSIFY-01] Classificação com tolerância de 0.1%
                    exit_reason = classify_exit(side_exit, exit_px, tp_px, sl_px)
                    pnl_pct = ((exit_px - entry_px) / entry_px * 100) if entry_px else 0.0
                    # Ajusta sinal do PnL para SHORT
                    if side_exit == "SHORT":
                        pnl_pct = -pnl_pct
                    logger.info(
                        "[%s] 🔚 FECHADO [%s] | entry=%s exit=%s pnl=%.2f%% | trade_id=%s",
                        inst, exit_reason,
                        format(entry_px, ".10g"), format(exit_px, ".10g"),
                        pnl_pct, data.get("trade_id"),
                    )
                    # ── Protections update ─────────────────────────────────────────
                    trade_margin = float(data.get('margin', 0) or 0)
                    self._record_close_for_protections(inst, exit_reason, pnl_pct, trade_margin)

                    log_event("position_closed",
                        symbol      = inst,
                        trade_id    = data.get("trade_id"),
                        entry_px    = round(entry_px, 8),
                        exit_px     = round(exit_px, 8),
                        exit_reason = exit_reason,
                        pnl_pct     = round(pnl_pct, 4),
                        signal      = data.get("signal"),
                        tp_px       = round(tp_px, 8),
                        sl_px       = round(sl_px, 8),
                    )
                # ── Fim do watcher de fechamento ─────────────────────────────

                # [BE-04] Auto Break-Even: verifica posições abertas ANTES do scanner
                # Chamado aqui para que o BE seja avaliado mesmo quando pos_count == max
                await self._check_break_even()

                if pos_count >= self._eff_max_positions:
                    await asyncio.sleep(self.config.LOOP_SECONDS)
                    continue

                # Saldo disponível, tickers e universo
                # [OPT-TICKERS] get_tickers() chamado 1x — reutilizado em _build_universe,
                # _pump_scan e no scanner do dashboard (sem chamada dupla).
                avail     = await self._get_avail()
                tickers   = await self.client.get_tickers() or []

                # [PUMP-01] Atualiza _pump_set com pumps detectados no ciclo atual
                self._pump_scan(tickers)

                universe  = await self._build_universe(tickers=tickers)

                # ── Sync _STATE para o dashboard ─────────────────────────────
                try:
                    pos_data = await self.client.get_positions() or []
                    positions_for_api = [
                        {
                            "instId":  p.get("instId", ""),
                            "sym":     p.get("instId", "").replace("-USDT-SWAP", ""),
                            "pos":     float(p.get("pos", 0) or 0),
                            "side":    "long" if float(p.get("pos", 0) or 0) > 0 else "short",
                            "avgPx":   float(p.get("avgPx", 0) or 0),
                            "markPx":  float(p.get("markPx", 0) or 0),
                            "liqPx":   float(p.get("liqPx", 0) or 0),
                            "upl":     float(p.get("upl", 0) or 0),
                            "imr":     float(p.get("imr", 0) or 0),
                        }
                        for p in pos_data if abs(float(p.get("pos", 0) or 0)) > 0
                    ]
                    # [OPT-TICKERS] Reutiliza tickers já buscados — sem chamada extra
                    tickers_top = sorted(
                        [t for t in tickers if "-USDT-SWAP" in t.get("instId", "")],
                        key=lambda t: float(t.get("volCcy24h", 0) or 0),
                        reverse=True
                    )[:20]
                    scanner_for_api = [
                        {
                            "sym":  t.get("instId", "").replace("-USDT-SWAP", ""),
                            "last": float(t.get("last", 0) or 0),
                            "pct":  (float(t.get("last", 0) or 0) - float(t.get("open24h", 0) or 0))
                                    / max(float(t.get("open24h", 0) or 1), 1e-12) * 100,
                        }
                        for t in tickers_top
                    ]
                    with _STATE_LOCK:
                        _STATE["equity"]     = equity
                        _STATE["avail"]      = avail if avail else 0.0
                        _STATE["daily_open"] = float(self.initial_equity) if self.initial_equity else equity
                        _STATE["running"]    = True
                        _STATE["positions"]  = positions_for_api
                        _STATE["scanner"]    = scanner_for_api
                except Exception as _se:
                    logger.debug("_STATE sync error: %s", _se)
                # ── Fim sync _STATE ───────────────────────────────────────────

                # ── Cálculo de notional e margem (base, sem risk parity) ───────
                # [FIX-P0-SCOPE] atr_pct é calculada por-símbolo em _regime_filters_ok().
                # Risk parity é aplicada DENTRO do loop por-símbolo (após atr_pct estar disponível).
                atr_pct = None   # inicializa aqui; será preenchida por _regime_filters_ok()
                if self.config.NOTIONAL_USD > 0:
                    trade_notional = self.config.NOTIONAL_USD * self.config.LEVERAGE
                    trade_margin   = self.config.NOTIONAL_USD
                else:
                    cota_avail   = (avail if avail and avail > 0 else equity) / self._eff_max_positions
                    trade_notional = cota_avail * self.config.LEVERAGE * 0.95
                    trade_margin   = trade_notional / self.config.LEVERAGE

                logger.info(
                    "⚙️  Sizing base: notional=%.2f USDT | margem=%.2f USDT | avail=%.2f USDT",
                    trade_notional, trade_margin, avail if avail else 0,
                )
                # ─────────────────────────────────────────────────────────────

                for ticker in universe:
                    if pos_count >= self._eff_max_positions:
                        break

                    instId = ticker.get("instId", "")
                    if not instId:
                        continue

                    # Pular se já tem posição aberta
                    if instId in self._open_inst_ids:
                        continue

                    # Info do instrumento
                    inst = await self.client.get_instrument_info(instId)
                    if not inst:
                        await asyncio.sleep(0.07)
                        continue

                    ct_val  = inst["ctVal"]
                    tick_sz = inst["tickSz"]
                    lot_sz  = inst["lotSz"]
                    min_sz  = inst["minSz"]

                    # ── [ENV-06] Cooldown por símbolo ─────────────────────────
                    last_ts = self._symbol_last_trade.get(instId, 0)
                    elapsed = _wall_time.time() - last_ts
                    if elapsed < self._eff_cooldown_sec:
                        remaining = int(self._eff_cooldown_sec - elapsed)
                        logger.debug("[%s] Cooldown ativo — %ds restantes", instId, remaining)
                        await asyncio.sleep(0.02)
                        continue
                    # ─────────────────────────────────────────────────────────

                    # ── Sinal técnico (BB + RSI + Volume + Pavio + 15m) ──────
                    signal, sig_meta = await self._get_signal(instId)
                    if signal is None:
                        await asyncio.sleep(0.07)
                        continue
                    # ── AT1: optional invert execution side (does NOT change TA) ──
                    if getattr(self.config, "INVERT_SIGNAL", False) and signal in ("buy", "sell"):
                        _orig = signal
                        signal = ("sell" if signal == "buy" else "buy")
                        logger.warning("⚠️ INVERT_SIGNAL ativo: %s → %s | %s", _orig.upper(), signal.upper(), instId)
                        try:
                            log_event("signal_inverted", instId=instId, orig=_orig, new=signal)
                        except Exception:
                            pass
                    # [FIX-P1-RSI] rsi_val e volume_mult agora chegam via sig_meta
                    _sig_rsi_val    = sig_meta.get("rsi_val", 0) if sig_meta else 0
                    _sig_vol_mult   = sig_meta.get("volume_mult", 1) if sig_meta else 1
                    _sig_c5         = sig_meta.get("c5") if sig_meta else None



                    # ── Global protections (StoplossGuard / DrawdownGuard) ─────
                    gblock, greason = self._global_guard_block()
                    if gblock:
                        # loga no máximo a cada 60s para não spammar
                        if self._now() - self._last_guard_log > 60:
                            logger.warning("⛔ Protections ativas: %s — entradas bloqueadas neste ciclo", greason)
                            self._last_guard_log = self._now()
                        await asyncio.sleep(0.02)
                        continue

                    # ── Pair lock (par ruim) ───────────────────────────────────
                    plock, preason = self._pair_locked(instId)
                    if plock:
                        logger.debug("[%s] 🔒 %s", instId, preason)
                        await asyncio.sleep(0.02)
                        continue

                    # ── Trend/Regime filters (HTF EMA + ADX/ATR%) ─────────────
                    ok_trend, trend_reason = await self._trend_filter_ok(instId, signal)
                    if not ok_trend:
                        logger.info("[%s] TrendFilter bloqueou: %s", instId, trend_reason)
                        log_event("decision", symbol=instId, signal=signal.upper(),
                                  decision="SKIP", reason="trend_filter", detail=trend_reason)
                        await asyncio.sleep(0.02)
                        continue

                    ok_reg, reg_reason, atr_pct = await self._regime_filters_ok(instId)
                    if not ok_reg:
                        logger.info("[%s] RegimeFilter bloqueou: %s", instId, reg_reason)
                        log_event("decision", symbol=instId, signal=signal.upper(),
                                  decision="SKIP", reason="regime_filter", detail=reg_reason)
                        await asyncio.sleep(0.02)
                        continue

                    # ── [FIX-P0-SCOPE] Risk Parity — ajusta margem por atr_pct ────
                    # Aplicada por-símbolo, após atr_pct estar disponível via _regime_filters_ok().
                    symbol_margin   = self._risk_parity_margin(trade_margin, atr_pct)
                    symbol_notional = symbol_margin * self.config.LEVERAGE
                    if symbol_margin != trade_margin:
                        logger.debug("[%s] RiskParity: margem ajustada %.2f→%.2f (atr_pct=%.4f)",
                                     instId, trade_margin, symbol_margin,
                                     atr_pct if atr_pct else 0)
                    vg_block, vg_reason = await self._btc_volatility_guard()
                    if vg_block:
                        logger.warning("[%s] ⏸ Volatility Guard: %s — pulando entradas", instId, vg_reason)
                        log_event("decision", symbol=instId, signal=signal.upper(),
                                  decision="SKIP", reason="volatility_guard",
                                  detail=vg_reason)
                        await asyncio.sleep(0.05)
                        continue

                    # ── Max same-direction positions ────────────────────────
                    if signal == "buy" and self._open_long_count >= self._eff_max_same_dir:
                        logger.info(
                            "[%s] ⛔ Max LONG atingido (%d/%d) — pulando",
                            instId, self._open_long_count, self._eff_max_same_dir
                        )
                        log_event("decision", symbol=instId, signal=signal.upper(),
                                  decision="SKIP", reason="max_same_direction",
                                  detail=f"long_count={self._open_long_count} max={self._eff_max_same_dir}")
                        await asyncio.sleep(0.03)
                        continue
                    if signal == "sell" and self._open_short_count >= self._eff_max_same_dir:
                        logger.info(
                            "[%s] ⛔ Max SHORT atingido (%d/%d) — pulando",
                            instId, self._open_short_count, self._eff_max_same_dir
                        )
                        log_event("decision", symbol=instId, signal=signal.upper(),
                                  decision="SKIP", reason="max_same_direction",
                                  detail=f"short_count={self._open_short_count} max={self._eff_max_same_dir}")
                        await asyncio.sleep(0.03)
                        continue

                    # ── Funding Gate ──────────────────────────────────────────
                    # Modo configurável via DALVAX_FUNDING_GATE_MODE
                    funding = await self.client.get_funding_rate(instId)
                    funding_ok = True
                    funding_reason = "ok"
                    if funding is not None and self.config.FUNDING_GATE_MODE != "off":
                        if self.config.FUNDING_GATE_MODE == "strict":
                            # Bloqueia se |funding| > MAX em qualquer direção
                            funding_ok = abs(funding) <= self.config.MAX_FUNDING_RATE
                            if not funding_ok:
                                funding_reason = f"strict: |{funding*100:.4f}%| > {self.config.MAX_FUNDING_RATE*100:.4f}%"
                        else:
                            # directional (padrão)
                            bloqueado = (
                                (signal == "buy"  and funding >  self.config.MAX_FUNDING_RATE) or
                                (signal == "sell" and funding < -self.config.MAX_FUNDING_RATE)
                            )
                            if bloqueado:
                                funding_ok = False
                                funding_reason = f"directional: signal={signal.upper()} funding={funding*100:.4f}%"

                    if not funding_ok:
                        logger.info(
                            "[%s] ⛔ Funding bloqueado: %s | limite=%.4f%%",
                            instId, funding_reason, self.config.MAX_FUNDING_RATE * 100,
                        )
                        log_event("decision", symbol=instId, signal=signal.upper(),
                                  decision="SKIP", reason="funding_gate",
                                  funding_pct=round((funding or 0)*100, 5),
                                  funding_mode=self.config.FUNDING_GATE_MODE)
                        await asyncio.sleep(0.07)
                        continue
                    else:
                        quem_paga = (
                            "pagando para ficar long"  if signal == "buy"  and (funding or 0) <= 0 else
                            "pagando para ficar short" if signal == "sell" and (funding or 0) >= 0 else
                            "funding neutro/favorável"
                        )
                        logger.info(
                            "[%s] ✅ Funding OK: signal=%s funding=%.4f%% (%s)",
                            instId, signal.upper(), (funding or 0) * 100, quem_paga,
                        )


                    # ── Correlation Gate (diversificação direcional) ──────────
                    corr_block, corr_reason = await self._correlation_gate(instId, signal)
                    if corr_block:
                        logger.info("[%s] ⛔ Correlation Gate: %s — pulando", instId, corr_reason)
                        log_event("decision", symbol=instId, signal=signal.upper(),
                                  decision="SKIP", reason="correlation_gate",
                                  detail=corr_reason)
                        await asyncio.sleep(0.05)
                        continue

                    # ── Mark price ───────────────────────────────────────────
                    mark_px = await self.client.get_mark_price(instId)
                    px = mark_px if mark_px and mark_px > 0 else float(ticker.get("last", 0) or 0)
                    if px <= 0:
                        logger.warning("[%s] ⚠️  Mark price inválido — pulando", instId)
                        continue

                    # ── Tamanho em contratos ──────────────────────────────────
                    sz = round_to_lot(symbol_notional / (px * ct_val), lot_sz)
                    logger.info(
                        "[%s] 📐 sz=%.4f | minSz=%.4f | px=%.6f | ctVal=%.6f | margem=%.2f",
                        instId, sz, min_sz, px, ct_val, symbol_margin,
                    )

                    if sz < min_sz:
                        logger.info("[%s] ⛔ sz=%s < minSz=%s — ignorado", instId, sz, min_sz)
                        continue

                    if avail is not None and avail < symbol_margin:
                        logger.warning(
                            "[%s] ⛔ Saldo insuf: avail=%.2f < margem=%.2f — ignorado",
                            instId, avail, symbol_margin,
                        )
                        continue

                    # Configurar alavancagem (FIX-06)
                    await self.client.set_leverage(instId)

                    # ── Calcular TP e SL — ATR dinâmico + RR garantido (FIX-TPPX + RR-01) ──
                    # CORREÇÃO CRÍTICA: tp_px/sl_px calculados ANTES do semi-auto (FIX-TPPX)
                    # MELHORIA RR: SL via ATR(14), TP = SL × MIN_RR (RR-01/02)
                    #
                    # Modo "atr_rr" (padrão):
                    #   sl_distance = ATR(14) × ATR_MULTIPLIER_SL  (cap: SL_MAX_PCT × px)
                    #   tp_distance = sl_distance × MIN_RR
                    #   RR efetivo ≥ 1.2 após fees (com MIN_RR=2.0 e 5x leverage)
                    #
                    # Fallback (ATR indisponível ou TP_MODE=fixed):
                    #   sl_distance = STOP_LOSS_PCT × px   (padrão: 1.2%)
                    #   tp_distance = TAKE_PROFIT_PCT × px (padrão: 2.4%)
                    #   RR efetivo ≈ 1.25 após fees (relatório 2)

                    rr_mode = "fixed"   # default se ATR falhar
                    sl_distance = px * self.config.STOP_LOSS_PCT
                    tp_distance = px * self.config.TAKE_PROFIT_PCT

                    if self.config.TP_MODE == "atr_rr":
                        try:
                            atr_candles = await self.client.get_candles(instId, self.config.BAR_INTERVAL, limit="30")
                            if atr_candles and len(atr_candles) >= 15:
                                atr_candles = list(reversed(atr_candles))
                                atr_val = calc_atr(atr_candles, period=14)
                                if atr_val and atr_val > 0:
                                    sl_dist_atr = atr_val * self._eff_atr_mult_sl
                                    # [RR-03] Cap: SL não pode exceder SL_MAX_PCT do preço
                                    sl_dist_atr = min(sl_dist_atr, px * self._eff_sl_max_pct)
                                    # [RR-02] TP garantido via MIN_RR
                                    tp_dist_atr = sl_dist_atr * self._eff_min_rr
                                    sl_distance = sl_dist_atr
                                    tp_distance = tp_dist_atr
                                    rr_mode = "atr_rr"
                        except Exception as _atr_err:
                            logger.debug("[%s] ATR falhou (%s) — usando fallback fixo", instId, _atr_err)

                    # Calcular preços de TP e SL conforme direção
                    if signal == "sell":
                        sl_px      = px + sl_distance
                        tp_px      = px - tp_distance
                        # Sanidade direcional
                        if tp_px >= px:
                            tp_px  = px * (1 - self.config.STOP_LOSS_PCT * 1.5)
                        if sl_px <= px:
                            sl_px  = px * (1 + self.config.STOP_LOSS_PCT)
                        close_side = "buy"
                    else:  # buy
                        sl_px      = px - sl_distance
                        tp_px      = px + tp_distance
                        # Sanidade direcional
                        if tp_px <= px:
                            tp_px  = px * (1 + self.config.STOP_LOSS_PCT * 1.5)
                        if sl_px >= px:
                            sl_px  = px * (1 - self.config.STOP_LOSS_PCT)
                        close_side = "sell"

                    # [RR-06][FIX-LOG-01] Log de RR check com %.10g — suporta ativos de preço
                    # muito baixo (ex: SATS ~1e-8) sem truncar para 0.0000.
                    # Inclui entry_px e min_rr para facilitar diagnóstico no Railway.
                    rr_real = tp_distance / sl_distance if sl_distance > 0 else 0
                    sl_pct_price = sl_distance / px * 100
                    tp_pct_price = tp_distance / px * 100
                    logger.info(
                        "[%s] 📐 RR check [%s]: entry=%.10g | SL=%.10g (%.3f%%) TP=%.10g (%.3f%%) RR=%.2f (min=%.2f)",
                        instId, rr_mode,
                        px, sl_px, sl_pct_price, tp_px, tp_pct_price,
                        rr_real, self._eff_min_rr,
                    )
                    # [FIX-RR-01] Guard com tolerância float (1e-9) e log MIN_RR_BLOCK explícito
                    if rr_real + 1e-9 < (self._eff_min_rr * 0.99):
                        logger.warning(
                            "[%s] ⛔ MIN_RR_BLOCK: rr=%.3f < min_rr=%.3f (mode=%s) — trade cancelado",
                            instId, rr_real, self._eff_min_rr, rr_mode,
                        )
                        continue
                    # ─────────────────────────────────────────────────────────

                    # [AT1] Guard anti-microtrade: exige alvo mínimo (TP% do preço)
                    # tp_pct_price está em percent (%)
                    if tp_pct_price + 1e-9 < float(self._eff_min_edge_pct):
                        logger.warning(
                            "[%s] ⛔ MIN_EDGE_BLOCK: tp_pct=%.3f%% < min_edge=%.3f%% — trade cancelado (fee/slippage)",
                            instId, tp_pct_price, self._eff_min_edge_pct,
                        )
                        continue

                    # ── MODO SEMI-AUTO: aguardar aprovação ─────────────────
                    if _STATE["mode"] == "semi-auto":
                        sig_id = str(uuid.uuid4())[:8]
                        pending = {
                            "id":       sig_id,
                            "instId":   instId,
                            "sym":      instId.replace("-USDT-SWAP", ""),
                            "side":     "long" if signal == "buy" else "short",
                            "entry":    px,
                            "tp":       tp_px,
                            "sl":       sl_px,
                            "sz":       sz,
                            "rsi":      round(_sig_rsi_val, 1),
                            "vm":       round(_sig_vol_mult, 2),
                            "conf":     min(int(sz * 10), 85),
                            "ts":       datetime.now(timezone.utc).strftime("%H:%M:%S"),
                            "decision": None,
                        }
                        with _STATE_LOCK:
                            _STATE["pending_signals"].append(pending)
                        _log_api(f"⏳ Sinal {instId} aguardando aprovação (5 min)...")

                        approved = False
                        for _ in range(self.config.SEMI_AUTO_TIMEOUT_SEC):
                            await asyncio.sleep(1)
                            with _STATE_LOCK:
                                dec = pending.get("decision")
                            if dec is not None:
                                approved = (dec == "accept")
                                break

                        with _STATE_LOCK:
                            try:
                                _STATE["pending_signals"].remove(pending)
                            except ValueError:
                                pass

                        if not approved:
                            _log_api(f"⏭ Sinal {instId} expirado/recusado — pulando", "INFO")
                            continue
                        _log_api(f"✅ Sinal {instId} aprovado — executando!", "INFO")
                    # ── FIM SEMI-AUTO ────────────────────────────────────────

                    # ── Gerar trade_id e clOrdId ─────────────────────────────
                    self._trade_seq += 1
                    trade_id  = uuid.uuid4().hex[:12]
                    cl_ord_id = f"DALVAX-{trade_id[:8]}"   # clOrdId para OKX (idempotência)

                    # [JOURNAL] decision=ENTER
                    log_event("decision",
                        symbol       = instId,
                        signal       = signal.upper(),
                        decision     = "ENTER",
                        trade_id     = trade_id,
                        entry_px     = round(px, 8),
                        sz           = sz,
                        notional_usd = round(symbol_notional, 2),
                        margin_usd   = round(symbol_margin, 2),
                        tp_px        = round(tp_px, 8),
                        sl_px        = round(sl_px, 8),
                        rr_mode      = rr_mode,
                        rr_ratio     = round(rr_real, 2),
                        sl_pct       = round(sl_pct_price, 4),
                        tp_pct       = round(tp_pct_price, 4),
                        funding_pct  = round((funding or 0) * 100, 5),
                        dry_run      = self.config.DRY_RUN,
                    )

                    # [ENV-04] DRY_RUN: loga tudo mas NÃO envia ordens
                    # [ENV-05] REAL_TRADING_ENABLED: chave mestra de segurança
                    can_trade = (not self.config.DRY_RUN) and self.config.REAL_TRADING_ENABLED
                    if not can_trade:
                        reason = "dry_run" if self.config.DRY_RUN else "real_trading_disabled"
                        logger.warning(
                            "[%s] 🔕 %s — sinal validado mas ordem NÃO enviada | trade_id=%s",
                            instId, reason.upper(), trade_id,
                        )
                        log_event("order_submit", symbol=instId, trade_id=trade_id,
                                  status="blocked", reason=reason,
                                  signal=signal.upper(), sz=sz, px=round(px,8))
                        await asyncio.sleep(0.07)
                        continue

                    # [JOURNAL] order_submit
                    log_event("order_submit",
                        symbol       = instId,
                        trade_id     = trade_id,
                        cl_ord_id    = cl_ord_id,
                        side         = signal,
                        sz           = sz,
                        entry_px     = round(px, 8),
                        notional_usd = round(symbol_notional, 2),
                        margin_usd   = round(symbol_margin, 2),
                        leverage     = self.config.LEVERAGE,
                        td_mode      = self.config.MARGIN_MODE,
                        simulated    = self.config.SIMULATED,
                    )

                    # ── Envio da ordem de entrada ─────────────────────────────
                    result = await self.client.place_order(instId, signal, sz)
                    if not result:
                        logger.warning("[%s] ⛔ place_order retornou vazio — ordem não confirmada pela OKX | trade_id=%s", instId, trade_id)
                        log_event("order_ack", symbol=instId, trade_id=trade_id,
                                  cl_ord_id=cl_ord_id, status="rejected", ord_id=None,
                                  s_code="empty", s_msg="empty response")
                        continue

                    # [JOURNAL] order_ack
                    try:
                        ord_id  = result[0].get("ordId", "?")
                        s_code  = result[0].get("sCode", "?")
                        s_msg   = result[0].get("sMsg", "")
                        ok      = str(s_code) == "0"
                        log_event("order_ack",
                            symbol    = instId,
                            trade_id  = trade_id,
                            cl_ord_id = cl_ord_id,
                            ord_id    = ord_id,
                            s_code    = s_code,
                            s_msg     = s_msg,
                            status    = "accepted" if ok else "rejected",
                        )
                        if ok:
                            logger.info("[%s] ✅ OKX confirmou ordem: ordId=%s | trade_id=%s", instId, ord_id, trade_id)
                        else:
                            logger.warning("[%s] ⚠️  OKX sCode=%s msg=%s | trade_id=%s", instId, s_code, s_msg, trade_id)
                    except Exception:
                        ord_id = "?"
                        logger.info("[%s] ✅ OKX retornou resultado (sem ordId parseável) | trade_id=%s", instId, trade_id)
                        log_event("order_ack", symbol=instId, trade_id=trade_id,
                                  cl_ord_id=cl_ord_id, status="unknown", ord_id=None)

                    logger.info(
                        "🎯 ORDEM %s: %s | Sz=%s contratos | Notional=%.2f USDT | Margem=%.2f USDT | trade_id=%s",
                        signal.upper(), instId, sz, symbol_notional, symbol_margin, trade_id,
                    )
                    pos_count += 1
                    self._open_inst_ids.add(instId)
                    self._symbol_last_trade[instId] = _wall_time.time()  # [ENV-06] cooldown
                    # [EXIT-02] Registra posição aberta para watcher de fechamento
                    # [BE-02]   algo_id e be_moved adicionados para o Auto Break-Even
                    # [FIX-BE-03] sz, close_side, tick_sz adicionados para fallback cancel+recreate
                    self._tracked_positions[instId] = {
                        "trade_id":   trade_id,
                        "entry_px":   float(px),
                        "tp_px":      float(tp_px),
                        "sl_px":      float(sl_px),
                        "margin":     float(symbol_margin),
                        "signal":     signal.upper(),
                        "ts":         _wall_time.time(),
                        "algo_id":    "",         # preenchido após OCO confirmado
                        "be_moved":   False,      # True após BE executado (evita dupla)
                        "sz":         sz,          # [FIX-BE-03] tamanho em contratos para recreate
                        "close_side": close_side,  # [FIX-BE-03] "buy" (SHORT) ou "sell" (LONG)
                        "tick_sz":    tick_sz,     # [FIX-BE-03] tick do instrumento para arredondamento
                    }

                    avail = (avail or 0) - symbol_margin

                    # ── OCO (TP/SL) ───────────────────────────────────────────
                    oco = await self.client.place_oco(
                        instId, close_side, sz, tp_px, sl_px, tick_sz
                    )
                    if oco:
                        algo_id = oco[0].get("algoId", "?") if isinstance(oco, list) else "?"
                        # [BE-02] Armazena algo_id para que _check_break_even possa amend
                        if instId in self._tracked_positions:
                            self._tracked_positions[instId]["algo_id"] = algo_id
                        logger.info("🛡️  TP=%.6f | SL=%.6f | Mark=%.6f | algoId=%s", tp_px, sl_px, px, algo_id)
                        log_event("oco_set",
                            symbol   = instId,
                            trade_id = trade_id,
                            ord_id   = ord_id,
                            algo_id  = algo_id,
                            tp_px    = round(tp_px, 8),
                            sl_px    = round(sl_px, 8),
                            mark_px  = round(px, 8),
                        )
                    else:
                        logger.warning("⚠️  Falha no OCO para %s — posição sem proteção automática! | trade_id=%s", instId, trade_id)
                        log_event("oco_set", symbol=instId, trade_id=trade_id,
                                  status="failed", tp_px=round(tp_px,8), sl_px=round(sl_px,8))

                    await asyncio.sleep(0.07)

            except Exception as exc:
                logger.error("Erro no loop principal: %s", exc, exc_info=True)
                await asyncio.sleep(10)

            await asyncio.sleep(self.config.LOOP_SECONDS)


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
# Adicionar ANTES do "async def main():" no bot original
# ══════════════════════════════════════════════════════════════════════════════

# ── Imports extras (adicionar ao topo do arquivo) ────────────────────────────

# ── Estado compartilhado Bot ↔ API ────────────────────────────────────────────
_STATE = {
    "mode":             env_str("DALVAX_MODE", "semi-auto"),
    "running":          False,
    "equity":           0.0,
    "avail":            0.0,
    "daily_open":       None,
    "positions":        [],
    "pending_signals":  [],
    "scanner":          [],
    "log_buffer":       deque(maxlen=300),
    "trades_cache":     [],
    "event_loop":       None,   # ref ao loop asyncio do bot (para chamar OKX do handler HTTP)
    "okx_client":       None,   # ref ao OkxClient instanciado
}

# ── Lock para acesso thread-safe ao _STATE ────────────────────────────────────
# O bot roda em asyncio (thread principal) e o handler HTTP roda em thread
# separada. O Lock garante que leituras/escritas ao _STATE não colidam.
_STATE_LOCK = threading.Lock()

# Rate-limit: /api/mode pode ser chamado no máximo 1x a cada 5 segundos
_RATE = {"mode_last": 0.0}
_RATE_LOCK = threading.Lock()

_API_SECRET = os.environ.get("DALVAX_API_SECRET", "").strip()
if not _API_SECRET or _API_SECRET == "dalvax2025":
    import secrets as _secrets
    _API_SECRET = _secrets.token_urlsafe(32)
    # [FIX-P0-SEC] Nunca logar o segredo completo — mostra só hint para diagnóstico.
    # Defina DALVAX_API_SECRET nas variáveis do Railway para valor fixo e estável.
    _secret_hint = _API_SECRET[:4] + "****"
    logger.warning(
        "⚠️  DALVAX_API_SECRET não definido ou fraco — gerado automaticamente (hint: %s). "
        "Defina DALVAX_API_SECRET no Railway para valor fixo.",
        _secret_hint,
    )
else:
    logger.info("✅ DALVAX_API_SECRET carregado das variáveis de ambiente.")

def _log_api(msg, level="INFO"):
    entry = {"ts": datetime.now(timezone.utc).strftime("%H:%M:%S"), "level": level, "msg": msg}
    with _STATE_LOCK:
        _STATE["log_buffer"].append(entry)
    getattr(logger, level.lower(), logger.info)(msg)

def _run_async(coro, timeout=12):
    """Executa coroutine asyncio a partir de uma thread síncrona (handler HTTP).
    Usa asyncio.run_coroutine_threadsafe para submeter ao event loop do bot."""
    loop = _STATE.get("event_loop")
    if loop is None or not loop.is_running():
        return None
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    try:
        return future.result(timeout=timeout)
    except Exception as exc:
        logger.error("_run_async error: %s", exc)
        return None

# ── HTTP Handler ─────────────────────────────────────────────────────────────
class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass  # suppress default log

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-API-Secret")

    def _json(self, data, code=200):
        body = _json.dumps(data).encode()
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _auth(self):
        return self.headers.get("X-API-Secret", "") == _API_SECRET

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        p = self.path.split("?")[0]
        if p == "/api/health":
            return self._json({"ok": True, "version": "3.1.3", "run_id": RUN_ID})

        if not self._auth():
            return self._json({"error": "unauthorized"}, 401)

        s = _STATE
        if p == "/api/status":
            dopen = s["daily_open"] or s["equity"]
            dpnl  = s["equity"] - dopen if dopen else 0
            return self._json({
                "mode":                s["mode"],
                "running":             s["running"],
                "equity":              s["equity"],
                "avail":               s["avail"],
                "daily_open":          dopen,
                "daily_pnl":           dpnl,
                "daily_pnl_pct":       dpnl / dopen * 100 if dopen else 0,
                "positions":           s["positions"],
                "pending_count":       len(s["pending_signals"]),
                "cb_limit_pct":        Config.MAX_DAILY_LOSS_PCT * 100,
                "max_positions":       Config.MAX_POSITIONS,
                "leverage":            Config.LEVERAGE,
                "margin_mode":         Config.MARGIN_MODE,
                "profile":             Config.RELAX_PROFILE,
                "simulated":           Config.SIMULATED,
                "dry_run":             Config.DRY_RUN,
                "real_trading":        Config.REAL_TRADING_ENABLED,
                "funding_mode":        Config.FUNDING_GATE_MODE,
                "funding_limit_pct":   Config.MAX_FUNDING_RATE * 100,
                "loop_sec":            Config.LOOP_SECONDS,
                "cooldown_sec":        Config.SYMBOL_COOLDOWN_SEC,
                "universe_size":       Config.MAX_UNIVERSE_SIZE,
                "run_id":              RUN_ID,
                "version":             "3.1.3",
                "tp_mode":             Config.TP_MODE,
                "atr_mult_sl":         Config.ATR_MULTIPLIER_SL,
                "min_rr":              Config.MIN_RR,
                "sl_max_pct":          Config.SL_MAX_PCT * 100,
            })

        if p == "/api/signals":
            return self._json(s["pending_signals"])

        if p == "/api/scanner":
            return self._json(s["scanner"])

        if p == "/api/logs":
            import urllib.parse
            qs = dict(urllib.parse.parse_qsl(self.path.split("?")[1] if "?" in self.path else ""))
            lim = int(qs.get("limit", 150))
            return self._json(list(s["log_buffer"])[-lim:])

        return self._json({"error": "not found"}, 404)

    def do_POST(self):
        if not self._auth():
            return self._json({"error": "unauthorized"}, 401)

        length = int(self.headers.get("Content-Length", 0))
        body   = _json.loads(self.rfile.read(length) or b"{}") if length else {}
        p      = self.path.split("?")[0]

        # ── Alterar modo ──────────────────────────────────────────────────────
        if p == "/api/mode":
            mode = body.get("mode", "")
            if mode not in ("auto", "semi-auto"):
                return self._json({"error": "invalid mode"}, 400)
            with _RATE_LOCK:
                now = _wall_time.monotonic()
                if now - _RATE["mode_last"] < 5.0:
                    return self._json({"error": "rate limit — aguarde 5s"}, 429)
                _RATE["mode_last"] = now
            with _STATE_LOCK:
                _STATE["mode"] = mode
            _log_api(f"🔄 Modo → {mode.upper()}", "INFO")
            return self._json({"ok": True, "mode": mode})

        # ── Fechar posição manualmente ─────────────────────────────────────────
        # POST /api/close_position
        # body: {"instId": "BTC-USDT-SWAP", "side": "long", "sz": "1"}
        if p == "/api/close_position":
            instId = body.get("instId", "")
            side   = body.get("side", "")    # "long" ou "short"
            sz     = body.get("sz", "")
            if not instId or not side or not sz:
                return self._json({"error": "instId, side e sz são obrigatórios"}, 400)
            close_side = "sell" if side == "long" else "buy"
            client = _STATE.get("okx_client")
            if not client:
                return self._json({"error": "cliente OKX não inicializado"}, 503)
            result = _run_async(client.place_order(instId, close_side, float(sz)))
            if result:
                _log_api(f"🔴 Posição fechada via dashboard: {instId} {side.upper()} sz={sz}", "INFO")
                return self._json({"ok": True, "result": result})
            return self._json({"error": "falha ao fechar posição — verifique logs"}, 500)

        # ── Atualizar TP/SL de posição ─────────────────────────────────────────
        # POST /api/update_tpsl
        # body: {"instId": "BTC-USDT-SWAP", "side": "long", "sz": "1", "tp": 95000, "sl": 88000}
        if p == "/api/update_tpsl":
            instId = body.get("instId", "")
            side   = body.get("side", "")
            sz     = body.get("sz", "")
            tp_px  = body.get("tp")
            sl_px  = body.get("sl")
            if not instId or not side or not sz:
                return self._json({"error": "instId, side e sz são obrigatórios"}, 400)
            close_side = "sell" if side == "long" else "buy"
            tick_sz    = body.get("tick_sz", "0.0001")
            client = _STATE.get("okx_client")
            if not client:
                return self._json({"error": "cliente OKX não inicializado"}, 503)
            result = _run_async(client.place_oco(
                instId, close_side, float(sz),
                float(tp_px) if tp_px else None,
                float(sl_px) if sl_px else None,
                tick_sz,
            ))
            if result:
                _log_api(f"🛡️  TP/SL atualizado via dashboard: {instId} TP={tp_px} SL={sl_px}", "INFO")
                return self._json({"ok": True, "result": result})
            return self._json({"error": "falha ao atualizar TP/SL — verifique logs"}, 500)

        # ── Aprovar / Recusar sinal pendente ──────────────────────────────────
        # POST /api/signals/<id>/accept  ou  /api/signals/<id>/reject
        parts = p.split("/")
        if len(parts) == 5 and parts[1] == "api" and parts[2] == "signals":
            sig_id   = parts[3]
            decision = parts[4]
            if decision not in ("accept", "reject"):
                return self._json({"error": "ação inválida"}, 400)
            with _STATE_LOCK:
                for sig in _STATE["pending_signals"]:
                    if sig["id"] == sig_id:
                        sig["decision"] = decision
                        emoji = "✅" if decision == "accept" else "❌"
                        _log_api(f"{emoji} Sinal {sig['sym']} {sig['side'].upper()} {decision.upper()} pelo dashboard")
                        return self._json({"ok": True})
            return self._json({"error": "sinal não encontrado"}, 404)

        return self._json({"error": "endpoint não encontrado"}, 404)

def _start_api_server():
    port = env_int("PORT", 8080)
    server = HTTPServer(("0.0.0.0", port), _Handler)
    logger.info("🌐 API HTTP escutando na porta %d", port)
    server.serve_forever()

# ── Iniciar API em thread background ─────────────────────────────────────────
_api_thread = threading.Thread(target=_start_api_server, daemon=True)
_api_thread.start()


async def main():
    # Registrar o event loop e cliente OKX no _STATE para uso pelo handler HTTP
    with _STATE_LOCK:
        _STATE["event_loop"] = asyncio.get_running_loop()
    bot = Bot(Config)
    with _STATE_LOCK:
        _STATE["okx_client"] = bot.client
    try:
        await bot.run()
    finally:
        await bot.client.close()


if __name__ == "__main__":
    asyncio.run(main())
