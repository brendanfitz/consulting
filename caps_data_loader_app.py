import textwrap
import tkinter as tk
from tkinter import filedialog
from tkinter.filedialog import askopenfilename
from tkinter import messagebox as mb
from caps_data_loader import CAPsDataLoader

class App(tk.Tk):

    def __init__(self):
        super().__init__()
        self.geometry("800x175")
        self.title("CAPs Data Downloader")
        self.title = self.create_title()

        self.run_frame = RunFrame(self)
        self.run_frame.pack(fill="both", expand=True, padx=5, pady=5)

    def create_title(self):
        title = tk.Label(text="CAPs Data Downloader", font=("Arial", 14, "bold"))
        title.pack(padx=10, pady=10)
        return title
    
    def end_frame(self, filepath):
        self.run_frame.destroy()
        end_frame = tk.Frame(self)
        end_frame.pack(fill="both", expand=True)

        label_var = tk.StringVar()
        filepath = textwrap.fill(filepath, 70)
        label_text = f"Data Load Complete.\nSee file located at:\n\n{filepath}"
        label = tk.Label(end_frame, textvariable=label_var)
        label_var.set(label_text)
        label.pack(fill="x", expand=True)
        return label, label_var
    
class RunFrame(tk.Frame):

    BUTTON_COLWIDTH = 20
    LABEL_COLWIDTH = 80

    def __init__(self, master):
        super().__init__(master, borderwidth=1, relief="groove")

        self.filepath = None
        self.file_label, self.file_label_var = self.create_file_label()
        self.file_btn = self.create_file_button()

        self.driver_filepath = None
        self.driver_file_label, self.driver_file_label_var = self.create_driver_file_label()
        self.driver_file_btn = self.create_driver_file_button()

        self.run_btn = self.create_run_button()

        self.file_btn.grid(row=0, column=0, sticky="NE", padx=5, pady=5)
        self.file_label.grid(row=0, column=1, sticky="NW", padx=5, pady=5)
        self.driver_file_btn.grid(row=1, column=0, sticky="NE", padx=5, pady=5)
        self.driver_file_label.grid(row=1, column=1, sticky="NW", padx=5, pady=5)
        self.run_btn.grid(row=2, column=0, columnspan=2, padx=5, pady=5)

    def create_file_label(self):
        file_label_var = tk.StringVar()

        file_label = tk.Label(self,
             textvariable=file_label_var,
             width=self.LABEL_COLWIDTH
        )
        file_label.configure(background="white")

        return file_label, file_label_var
    
    def create_driver_file_button(self):
        driver_file_btn = tk.Button(self,
            text='Select Chromedriver File',
            command=self.driver_file_dialog,
            width=self.BUTTON_COLWIDTH,
        )
        return driver_file_btn

    def create_driver_file_label(self):
        driver_file_label_var = tk.StringVar()

        driver_file_label = tk.Label(self,
            textvariable=driver_file_label_var,
            width=self.LABEL_COLWIDTH,
        )
        driver_file_label.configure(background="white")

        return driver_file_label, driver_file_label_var
    
    def create_file_button(self):
        file_btn = tk.Button(self,
            text='Select Data File',
            command=self.file_dialog,
            width=self.BUTTON_COLWIDTH,
        )
        return file_btn

    def file_dialog(self):
        self.filepath = filedialog.askopenfilename(
            parent=self,
            title='Choose a file',
            filetype = (("Excel file","*.xlsx"),("Excel file", "*.xls")),
        )
        self.file_label_var.set(textwrap.fill(self.filepath, self.LABEL_COLWIDTH - 2))

    def driver_file_dialog(self):
        self.driver_filepath = filedialog.askopenfilename(
            parent=self,
            title='Choose a file',
        )
        self.driver_file_label_var.set(textwrap.fill(self.driver_filepath, self.LABEL_COLWIDTH - 2))
    
    
    def create_run_button(self):
        run_btn = tk.Button(self, text="Run", command=self.run)
        return run_btn
    
    def run(self):
        if self.filepath is None:
           mb.showwarning('Error', "Please enter a filename")
        else:
            loader = CAPsDataLoader(self.filepath, self.driver_filepath)
            try:
                filename = loader.fetch_data()
                self.master.end_frame(filename)
            except FileNotFoundError:
                mb.showwarning(
                    'Error',
                    f"Place Chromedriver executable in {CAPsDataLoader.EXECUTABLE_PATH} directory"
                )
