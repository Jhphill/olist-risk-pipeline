# Bot de Trading — Guia Completa

## Estructura del proyecto

trading_bot/
  01_descarga_datos.py         <- PRIMERO: descarga historial de Binance
  02_backtest.py               <- SEGUNDO: valida la estrategia
  03_analisis_sensibilidad.py  <- TERCERO: verifica robustez
  04_bot_live.py               <- CUARTO: paper trading en vivo
  05_dashboard.py              <- Monitoreo de performance
  indicadores.py               <- Modulo de indicadores (no correr directamente)
  data/                        <- Datos historicos (se genera solo)
  results/                     <- Resultados de backtest
  logs/                        <- Logs del bot en vivo

FASE 0 - Instalacion
pip install ccxt pandas numpy pandas-ta vectorbt scipy scikit-learn xgboost requests python-dotenv

FASE 1 - Backtest (Semanas 1-4)
  1. python 01_descarga_datos.py
  2. python 02_backtest.py
     Metas: Sharpe > 0.7, PF > 1.2, MaxDD < 20%, trades > 100
  3. python 03_analisis_sensibilidad.py
     Si degradacion < 30% en todos -> sistema robusto

FASE 2 - Paper Trading (Semanas 5-10)
  1. python 04_bot_live.py       (modo PAPER por defecto)
  2. python 05_dashboard.py      (monitoreo)
  NO cambies parametros durante 8 semanas aunque haya rachas malas

FASE 3 - Dinero Real (solo si FASE 2 fue exitosa)
  Crear .env con BINANCE_API_KEY y BINANCE_SECRET
  MODO=REAL python 04_bot_live.py

Para VPS (24/7): usar screen o systemd en Ubuntu 22.04
