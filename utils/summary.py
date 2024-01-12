def calculate_max_drawdown(capital_over_time):
    peak = capital_over_time[0]
    max_drawdown = 0
    for capital in capital_over_time:
        peak = max(peak, capital)
        drawdown = (peak - capital) / peak
        max_drawdown = max(max_drawdown, drawdown)
    return max_drawdown * 100  # Convert to percentage

