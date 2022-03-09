import quantopian.algorithm as algo
import quantopian.optimize as opt
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import Returns
from quantopian.pipeline.filters import QTradableStocksUS

MAX_GROSS_EXPOSURE = 2.0

MAX_POSITION_CONCENTRATION = 0.01

RETURNS_LOOKBACK_DAYS = 10

def initialize(context):

    algo.schedule_function(
        rebalance,
        algo.date_rules.week_start(days_offset=0),
        algo.time_rules.market_open(hours=1, minutes=30)
    )

    algo.attach_pipeline(make_pipeline(context), 'mean_reversion')

def make_pipeline(context):

    universe = QTradableStocksUS()
    
    recent_returns = Returns(
        window_length=RETURNS_LOOKBACK_DAYS, 
        mask=universe
    )
    
    recent_returns_zscore = recent_returns.zscore()

    low_returns = recent_returns_zscore.percentile_between(0,5)
    high_returns = recent_returns_zscore.percentile_between(95,100)

    securities_to_trade = (low_returns | high_returns)

     pipe = Pipeline(
        columns={
            'recent_returns_zscore': recent_returns_zscore
        },
        screen=securities_to_trade
    )

    return pipe

def before_trading_start(context, data):

    context.output = algo.pipeline_output('mean_reversion')

    context.recent_returns_zscore = context.output['recent_returns_zscore']


def rebalance(context, data):

    objective = opt.MaximizeAlpha(-context.recent_returns_zscore)

    max_gross_exposure = opt.MaxGrossExposure(MAX_GROSS_EXPOSURE)
    
     max_position_concentration = opt.PositionConcentration.with_equal_bounds(
        -MAX_POSITION_CONCENTRATION,
        MAX_POSITION_CONCENTRATION
    )
    
    dollar_neutral = opt.DollarNeutral()
    
    constraints = [
        max_gross_exposure,
        max_position_concentration,
        dollar_neutral,
    ]

    algo.order_optimal_portfolio(objective, constraints)