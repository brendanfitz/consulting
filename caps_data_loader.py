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

    EXECUTABLE_PATH = r'%s\Documents\chromedriver.exe' % os.path.expanduser('~')
    dl_dir = r'%s\Downloads' % os.path.expanduser('~')
    URL = r'http://mcdc.missouri.edu/applications/capsACS.html'

    def __init__(self, filename, chromedriver_filename):
        self.filename = filename
        if not os.path.isfile(self.EXECUTABLE_PATH):
            raise FileNotFoundError()
        self.browser = webdriver.Chrome(executable_path=chromedriver_filename)

    def fetch_data(self):
        df_cords = (pd.read_excel(self.filename, engine="openpyxl")
                    .drop_duplicates())
    
        coordinate_list = self.get_coordinate_list(df_cords)
        try:
            for coordinates in coordinate_list:
                self.download_caps_file(coordinates)
        finally:
            filename = self.compile_caps_files()
            self.browser.close()
            return filename
    
    def compile_caps_files(self):
        csv_pat = re.compile(r'^capsACS.*\.csv$')
        files = [self.dl_dir + r'\%s' % x for x in os.listdir(self.dl_dir) if csv_pat.match(x)]
        df_scrape = (pd.concat([pd.read_csv(x) for x in files])
            .pipe(self.split_cords)
            .rename(columns={'radius': 'Radius'})
            .set_index(['Longitude', 'Latitude', 'Radius'])
        )

        timestamp = dt.datetime.now().isoformat()[0:-7].replace(':', '.')
        out_fname = self.dl_dir + r'\CAPS Data_%s.xlsx' % timestamp
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
        time.sleep(5)
    
        (self.browser.find_element_by_xpath('//*[@id="body"]/p[1]/a')
         .click())
        time.sleep(5)
    
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