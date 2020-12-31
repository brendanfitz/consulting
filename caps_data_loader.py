# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 15:52:56 2019

@author: brendan.fitzpatrick
"""

import os
import sys
from selenium import webdriver
import pandas as pd
import re
import time
import datetime as dt
import warnings

class CAPsDataLoader(object):

    DOWNLOADS_DIR = os.path.join('Program Data')
    URL = r'http://mcdc.missouri.edu/applications/capsACS.html'
    APP_DIR = os.path.expandvars(os.path.join('%APPDATA%', 'CAPS Data Loader'))
    CHROMEDRIVER_SAVED_FILEPATH = os.path.join(APP_DIR,
        'chrome_driver_location.txt'
    )

    def __init__(self, filename, chromedriver_filename):
        self.runtime = dt.datetime.now()
        self.file_downloads_dir = os.path.join(
            self.APP_DIR,
            self.runtime.strftime('%Y-%m-%d'),
            self.runtime.strftime('%H%M'),
        )
        os.makedirs(self.file_downloads_dir)

        self.filename = filename

        self.create_directories()

        if chromedriver_filename is not None:
            self.chromedriver_filename = chromedriver_filename

            with open(self.CHROMEDRIVER_SAVED_FILEPATH, 'w') as f:
                f.write(self.chromedriver_filename)
        elif os.path.isfile(self.CHROMEDRIVER_SAVED_FILEPATH):
            self.chromedriver_filename = self.load_saved_chromedriver_filename()
        else:
            raise ValueError("Please provide chrome driver file")

        if not os.path.isfile(self.chromedriver_filename):
            raise FileNotFoundError()

        self.browser = webdriver.Chrome(executable_path=self.chromedriver_filename)


    @classmethod
    def create_directories(cls):
        if not os.path.isdir(cls.APP_DIR):
            os.mkdir(cls.APP_DIR)

        if not os.path.isdir(cls.DOWNLOADS_DIR):
            os.mkdir(cls.DOWNLOADS_DIR)
        
    @classmethod
    def load_saved_chromedriver_filename(cls):
        if os.path.isfile(cls.CHROMEDRIVER_SAVED_FILEPATH):
            with open(cls.CHROMEDRIVER_SAVED_FILEPATH) as f:
                saved_filepath = f.read()
            return saved_filepath

    def fetch_data(self):
        df_cords = (pd.read_excel(self.filename, engine="openpyxl")
                    .drop_duplicates())
    
        coordinate_list = self.get_coordinate_list(df_cords)
        try:
            for coordinates in coordinate_list:
                self.download_caps_file(coordinates)
        except Exception as e:
            raise e
        finally:
            filename = self.compile_caps_files()
            self.browser.close()
            return filename
    
    def compile_caps_files(self):
        csv_pat = re.compile(r'^capsACS.*\.csv$')

        files = list()
        for filename in os.listdir(self.file_downloads_dir):
            if csv_pat.match(filename):
                filepath = os.path.join(self.file_downloads_dir, filename)
                files.append(filepath)

        df_scrape = (pd.concat([pd.read_csv(x) for x in files])
            .pipe(self.split_cords)
            .rename(columns={'radius': 'Radius'})
            .set_index(['Longitude', 'Latitude', 'Radius'])
        )

        timestamp = dt.datetime.now().isoformat()[0:-7].replace(':', '.')
        out_fname = os.path.join(
            self.DOWNLOADS_DIR,
            f'CAPS Data_{timestamp}.xlsx'
        )
        df_scrape.to_excel(out_fname, merge_cells=False)

        for file in files:
            os.remove(file)
        print('See file located at {:s}'.format(out_fname))

        return out_fname
    
    def download_caps_file(self, coordinates):
        latitude, longitude, radius = coordinates
        self.browser.get(self.URL)
        (self.browser.find_element_by_xpath('//*[@id="latitude"]')
         .send_keys(str(latitude)))
        (self.browser.find_element_by_xpath('//*[@id="longitude"]')
         .send_keys(str(longitude)))
        (self.browser.find_element_by_xpath('//*[@id="radii"]')
         .send_keys(str(radius)))
    
        (self.browser.find_element_by_xpath('//*[@id="body"]/div/form/div/input[1]')
         .click())
        time.sleep(3)

        file_download_btn = self.browser.find_element_by_xpath('//*[@id="body"]/p[1]/a')
        file_download_btn.click()
        time.sleep(3)

        self.move_file_download(file_download_btn)

    def move_file_download(self, file_download_btn):
        filename = file_download_btn.get_property('href').split('/')[-1]
        src_filepath = os.path.join(os.path.expanduser('~'),
            'Downloads',
            filename,
        )
        dest_filepath = os.path.join(self.file_downloads_dir, filename)
        os.rename(src_filepath, dest_filepath)
    
    @staticmethod
    def get_coordinate_list(df):
        coordinates = (df.loc[:, ['Latitude', 'Longitude', 'Radius']]
            .values
            .tolist()
        )
        return coordinates
        
    @staticmethod
    def split_cords(input_df):
        df = input_df.copy()
    
        df[['Longitude', 'Latitude']] = (df.sitename
            .str.replace('(', '')
            .str.replace(')', '')
            .str.strip()
            .str.split(', ', expand=True)
            .rename(columns={0: 'Longitude', 1: 'Latitude'})
            .astype('float64')
        )
        return df

if __name__ == '__main__':
    filename = sys.argv[0]
    loader = CAPsDataLoader(filename)
    loader.fetch_caps_data()