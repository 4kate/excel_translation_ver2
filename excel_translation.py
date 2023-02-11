import pandas as pd
import multiprocessing as mp
import numpy as np
import os
from googletrans import Translator #use 3.1.0a0 or later


my_excel = 'OP10-40-texts.xlsx'
my_excel_sheet_name="User Texts"
n_cores = 4
result_excel = f'{my_excel[:-5]}_transleted.xlsx'
language = 'en'
original_to_translation = "es-ES"
destination_to_translation = "en-US"


def process_frame(df_chunk):
    # process data frame
    df_chunk.fillna("",inplace=True)

    #create replace column, same as original - we get rid of old translation if exist
    df_chunk[destination_to_translation] = df_chunk[original_to_translation]

    #translate this column
    print('Translating chunk...')
    translator = Translator()
    df_chunk[destination_to_translation] = df_chunk[destination_to_translation].apply(lambda x : translator.translate(x, dest = language).text)

    return df_chunk

if __name__ == '__main__':

    read_file = pd.read_excel(my_excel, my_excel_sheet_name)

    df_split = np.array_split(read_file, n_cores)
    pool = mp.Pool(n_cores) # use processes

    # process each data frame
    result_df = pd.concat(pool.map(process_frame, df_split))
    #joining data frames
    print('Concatenating chunk...')
    pool.close()
    pool.join()

    if os.path.exists(result_excel):
        print(f'REMOVED file {result_excel}')
        
    # export result df to excel
    result_df.to_excel(f'{result_excel}',index=False)  
    print('FINITO !!!')
