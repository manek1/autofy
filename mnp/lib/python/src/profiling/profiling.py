import glob
import os
import pandas as pd
from sqlalchemy import create_engine
import xlsxwriter

class Profiling:
    def __init__(self, source_type: str = 'csv', src_file_path:str=None, profiling_target_type:str = 'excel',
                 profiling_file_path:str=None, profiling_file_name: str='profiling.xlsx', src_db_url: str=None,
                 src_table_name:str=None, profiling_db_url: str=None,profiling_table_name:str=None):
        self.source_type = source_type
        self.src_file_path = src_file_path
        self.profiling_file_path = profiling_file_path
        self.profiling_file_name = profiling_file_name
        self.profiling_target_type = profiling_target_type
        self.src_db_url = src_db_url
        self.src_table_name = src_table_name
        self.profiling_db_url = profiling_db_url
        self.profiling_table_name = profiling_table_name
        self.profiling_file_full_path = os.path.join(self.profiling_file_path, self.profiling_file_name)
        if self.src_db_url:
            self.src_engine = create_engine(self.src_db_url)
        if self.profiling_db_url:
            self.profiling_engine = create_engine(self.src_db_url)

        self.profiling = {
            "Column": [],
            "Null Count": [],
            "Unique Count": [],
            "Data Type": [],
            "Min": [],
            "Max": []
        }

    def create_source_df(self, read_path=None):
        path = read_path if read_path else self.src_file_path
        try:
            df = pd.DataFrame()
            if self.source_type == 'csv':
                df = pd.read_csv(path)
            elif self.source_type == 'excel':
                df = pd.read_excel(path)
            elif self.source_type == 'sql':
                df = pd.read_sql_table(self.src_table_name, con=self.src_engine)
            return df

        except Exception as e:
            print(e)
            return None

    def get_profiling_df(self, df: pd.DataFrame):

        for col in df.columns:
            self.profiling["Column"].append(col)
            self.profiling["Null Count"].append(df[col].isnull().sum())
            self.profiling["Unique Count"].append(df[col].nunique())
            self.profiling["Data Type"].append(df[col].dtype)

            if pd.api.types.is_numeric_dtype(df[col]):
                self.profiling["Min"].append(df[col].min())
                self.profiling["Max"].append(df[col].max())
            else:
                self.profiling["Min"].append("")
                self.profiling["Max"].append("")

        profile_df = pd.DataFrame(self.profiling)

        return profile_df


    def write_profiling_df(self, df_list: list, sheet_name: list) -> None:
        print(f'writing {sheet_name} to {self.profiling_file_full_path} ')
        if self.profiling_target_type == 'excel':
            print("Writing excel profiling file")
            with pd.ExcelWriter(self.profiling_file_full_path, engine="xlsxwriter") as writer:
                for df, src_name in zip(df_list, sheet_name):
                    df.to_excel(writer, sheet_name=src_name, index=False)
        elif self.profiling_target_type == 'sql':
            print("Writing sql profiling file")
            for df, src_name in zip(df_list, sheet_name):
                df['source_name'] = src_name
                df.to_sql(self.profiling_table_name, con=self.profiling_engine, if_exists='append', index=False)
        else:
            raise NotImplementedError("profiling_target_type must be 'excel' or 'sql'")


    def profile_data(self):
        print("Profiling...")
        input_file_path = list()
        profiling_df_list = list()
        profiling_sheet_name_list = list()
        if self.source_type == 'csv' or self.source_type == 'xlsx':
            print("Profiling for csv files")
            if '.' not in self.src_file_path.split('/'):
                input_file_path = glob.glob(f'{os.path.join(self.src_file_path, '*.' + self.source_type)}') # input("Enter the file path: ")
            else:
                input_file_path = glob.glob(self.src_file_path)

        for input_file in input_file_path:
            split_param = "\\" if "\\" in input_file else "/"
            profiling_sheet_name_list.append(input_file.split(split_param)[-1].replace(".csv", ""))
            print("reading file ", input_file)
            file_df = self.create_source_df(input_file)
            print(file_df)
            profiling_df_list.append(self.get_profiling_df(file_df))

        self.write_profiling_df(profiling_df_list, profiling_sheet_name_list)


if __name__ == '__main__':
    prof = Profiling(src_file_path="src_file", profiling_file_path="profiling_output")
    prof.profile_data()