# Read the file
with open('src/open_data/core/timeseries.py', 'r') as f:
    content = f.read()

# Replace the buggy lines
old_code = """        # Calculate growth rates
        growth_rates = np.diff(values) / values[:-1] * 100
        avg_growth_rate = np.nanmean(growth_rates)"""

new_code = """        # Calculate growth rates
        # For data that's already a growth rate (like GDP growth %), just average it
        # For absolute values (like GDP amount), calculate year-over-year changes
        if abs(np.nanmean(values)) < 100:  # Likely already a percentage/rate
            avg_growth_rate = np.nanmean(values)
            growth_rates = values  # Already growth rates
        else:  # Absolute values
            growth_rates = np.diff(values) / values[:-1] * 100
            avg_growth_rate = np.nanmean(growth_rates)"""

content = content.replace(old_code, new_code)

# Write back
with open('src/open_data/core/timeseries.py', 'w') as f:
    f.write(content)

print("✓ Fixed!")
