import os
import pandas as pd
import re
from multiprocessing import Pool

input_directory = r'\\dev29\SharedFolder\Kushal_data\JAINAM BROKING LIMITED\CASH\NSE STOCK\2020\2020\MAR_2020'
output_directory = r'\\dev29\SharedFolder\Kushal_data\CASH\NSE STOCK\2020\MAR'
def get_stock_name(option_symbol):
    match = re.search(r'^(.+?)\.', option_symbol)
    if match:
        return match.group(1)
    return ""
def process_csv_file(file_path, output_directory):
    df = pd.read_csv(file_path)
    df['Stock'] = df['Ticker'].apply(get_stock_name)
    unique_stocks = df['Stock'].unique()
    for stock in unique_stocks:
        stock_data = df[df['Stock'] == stock]
        if not stock_data.empty:
            date = stock_data['Date'].iloc[0]
            formatted_date = date.replace("-", "").replace("/", "")
            stock_folder = os.path.join(output_directory, stock)
            os.makedirs(stock_folder, exist_ok=True)  

            output_filename = os.path.join(stock_folder, f'{stock}_{formatted_date}.csv')
            # Save the stock's data to a new CSV file in the stock folder with the desired name
            stock_data.to_csv(output_filename, index=False)
def main():
    files = [os.path.join(input_directory, filename) for filename in os.listdir(input_directory) if filename.endswith('.csv')]
    # Use multiprocessing to parallelize processing
    with Pool() as pool:
        pool.starmap(process_csv_file, [(file, output_directory) for file in files])
        print(f"Data from multiple CSV files have been processed and saved to respective folders with the desired naming format.")
if __name__ == '__main__':
    main()