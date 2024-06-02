import asyncio
import pandas as pd
from google.cloud import bigquery
import plotly.graph_objs as go
import tkinter as tk
from tkinter import ttk, filedialog
from fpdf import FPDF
from datetime import datetime

# Initialize a BigQuery client
client = bigquery.Client()

# Define the ITEMIDs for the ten attributes
item_ids = (211, 220045, 51, 220179, 8368, 220180, 52, 220181, 618, 220210, 646, 220277, 678, 223761, 113, 220074, 807, 220621, 40055)

# Define your query
async def fetch_data():
    query = f"""
        SELECT
            t1.SUBJECT_ID,
            t2.ICUSTAY_ID,
            t3.LABEL,
            t1.VALUE,
            t1.VALUEUOM,
            t1.CHARTTIME,
            t2.LOS,
            t4.DIAGNOSIS,
            t5.GENDER,
            t5.DOB,
            t6.ADMITTIME
        FROM
            `cdla-trabalho.CHARTEVENTS.CHARTEVENTS` AS t1
        INNER JOIN
            `cdla-trabalho.CHARTEVENTS.ICUSTAYS` AS t2
        ON
            t1.ICUSTAY_ID = t2.ICUSTAY_ID
        INNER JOIN
            `cdla-trabalho.CHARTEVENTS.D_ITEMS` AS t3
        ON
            t1.ITEMID = t3.ITEMID
        LEFT JOIN
            `cdla-trabalho.CHARTEVENTS.ADMISSIONS` AS t4
        ON
            t2.HADM_ID = t4.HADM_ID
        INNER JOIN
            `cdla-trabalho.CHARTEVENTS.PATIENTS` AS t5
        ON
            t1.SUBJECT_ID = t5.SUBJECT_ID
        LEFT JOIN
            `cdla-trabalho.CHARTEVENTS.ADMISSIONS` AS t6
        ON
            t2.HADM_ID = t6.HADM_ID
        WHERE 
            t2.SUBJECT_ID = 249 
            AND t1.ERROR = 0
            AND t1.ITEMID IN {item_ids}
        ORDER BY t2.ICUSTAY_ID, t1.CHARTTIME ASC 
    """

    query_job = client.query(query)
    results = query_job.result()

    return results.to_dataframe()

class PlotterApp:
    def __init__(self, root, results):
        self.root = root
        self.results = results
        self.stays = results['ICUSTAY_ID'].unique()
        self.current_stay_index = 0

        self.root.title("ICU Data Plotter")

        self.plot_frame = tk.Frame(self.root)
        self.plot_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=1)

        self.button_frame = tk.Frame(self.root)
        self.button_frame.pack(side=tk.BOTTOM)

        self.prev_button = tk.Button(self.button_frame, text="Previous", command=self.prev_stay)
        self.prev_button.pack(side=tk.LEFT)

        self.next_button = tk.Button(self.button_frame, text="Next", command=self.next_stay)
        self.next_button.pack(side=tk.LEFT)

        self.resume_button = tk.Button(self.button_frame, text="Resume", command=self.show_resume)
        self.resume_button.pack(side=tk.LEFT)

        self.compare_button = tk.Button(self.button_frame, text="Comparative Analysis", command=self.show_comparative_analysis)
        self.compare_button.pack(side=tk.LEFT)

        self.export_button = tk.Button(self.button_frame, text="Export as PDF", command=self.export_as_pdf)
        self.export_button.pack(side=tk.LEFT)

        self.plot_stay()

    def plot_stay(self):
        stay = self.stays[self.current_stay_index]
        stay_results = self.results[self.results['ICUSTAY_ID'] == stay]
        diagnosis = stay_results['DIAGNOSIS'].iloc[0] if 'DIAGNOSIS' in stay_results.columns else "No diagnosis info"

        fig = go.Figure()
        grouped_stay_results = stay_results.groupby('LABEL')

        for label, stay_values in grouped_stay_results:
            x = pd.to_datetime(stay_values['CHARTTIME'])
            y = stay_values['VALUE']
            fig.add_trace(go.Scatter(x=x, y=y, mode='lines', name=label))

        fig.update_layout(
            title=f'Subject ID: {stay_results["SUBJECT_ID"].iloc[0]}, ICU Stay ID: {stay}<br>Diagnosis: {diagnosis}',
            xaxis_title='Time',
            yaxis_title='Value'
        )

        self.show_plot(fig)

    def show_plot(self, fig):
        for widget in self.plot_frame.winfo_children():
            widget.destroy()

        canvas = fig.to_image(format="png")
        image = tk.PhotoImage(data=canvas)
        label = tk.Label(self.plot_frame, image=image)
        label.image = image
        label.pack()

    def prev_stay(self):
        if self.current_stay_index > 0:
            self.current_stay_index -= 1
            self.plot_stay()

    def next_stay(self):
        if self.current_stay_index < len(self.stays) - 1:
            self.current_stay_index += 1
            self.plot_stay()

    def show_resume(self):
        stay = self.stays[self.current_stay_index]
        stay_results = self.results[self.results['ICUSTAY_ID'] == stay]
        diagnosis = stay_results['DIAGNOSIS'].iloc[0] if 'DIAGNOSIS' in stay_results.columns else "No diagnosis info"

        # Calculate age
        dob = pd.to_datetime(stay_results['DOB'].iloc[0])
        admit_time = pd.to_datetime(stay_results['ADMITTIME'].iloc[0])
        age = (admit_time - dob).days // 365

        resume_window = tk.Toplevel(self.root)
        resume_window.title("Patient ICU Stay Summary")

        ttk.Label(resume_window, text=f"Subject ID: {stay_results['SUBJECT_ID'].iloc[0]}").pack(pady=10)
        ttk.Label(resume_window, text=f"ICU Stay ID: {stay}").pack(pady=10)
        ttk.Label(resume_window, text=f"Diagnosis: {diagnosis}").pack(pady=10)
        ttk.Label(resume_window, text=f"Length of Stay (LOS): {stay_results['LOS'].iloc[0]:.2f} days").pack(pady=10)
        ttk.Label(resume_window, text=f"Gender: {stay_results['GENDER'].iloc[0]}").pack(pady=10)
        ttk.Label(resume_window, text=f"Age: {age} years").pack(pady=10)

        stats = stay_results.groupby('LABEL')['VALUE'].agg(['mean', 'median', 'std', 'min', 'max'])
        ttk.Label(resume_window, text="Vital Sign Statistics:").pack(pady=10)
        for label, row in stats.iterrows():
            text = f"{label} - Mean: {row['mean']:.2f}, Median: {row['median']:.2f}, Std: {row['std']:.2f}, Min: {row['min']:.2f}, Max: {row['max']:.2f}"
            ttk.Label(resume_window, text=text).pack(pady=2)

    def show_comparative_analysis(self):
        stay = self.stays[self.current_stay_index]
        stay_results = self.results[self.results['ICUSTAY_ID'] == stay]

        comparison_window = tk.Toplevel(self.root)
        comparison_window.title("Comparative Analysis")

        normal_ranges = {
            "Heart Rate": (60, 100),
            "SpO2": (95, 100),
            "Temperature": (36.1, 37.2),
            "Systolic Blood Pressure": (90, 120),
            "Diastolic Blood Pressure": (60, 80),
            "Respiratory Rate": (12, 20),
            "Glucose": (70, 140)
        }

        grouped_stay_results = stay_results.groupby('LABEL')

        ttk.Label(comparison_window, text="Comparative Analysis with Normal Ranges:").pack(pady=10)
        for label, stay_values in grouped_stay_results:
            if label in normal_ranges:
                mean_value = stay_values['VALUE'].mean()
                normal_range = normal_ranges[label]
                text = f"{label} - Mean: {mean_value:.2f} (Normal Range: {normal_range[0]} - {normal_range[1]})"
                ttk.Label(comparison_window, text=text).pack(pady=2)

    def export_as_pdf(self):
        file_path = filedialog.asksaveasfilename(defaultextension=".pdf", filetypes=[("PDF files", "*.pdf")])
        if not file_path:
            return

        stay = self.stays[self.current_stay_index]
        stay_results = self.results[self.results['ICUSTAY_ID'] == stay]
        diagnosis = stay_results['DIAGNOSIS'].iloc[0] if 'DIAGNOSIS' in stay_results.columns else "No diagnosis info"

        # Calculate age
        dob = pd.to_datetime(stay_results['DOB'].iloc[0])
        admit_time = pd.to_datetime(stay_results['ADMITTIME'].iloc[0])
        age = (admit_time - dob).days // 365

        stats = stay_results.groupby('LABEL')['VALUE'].agg(['mean', 'median', 'std', 'min', 'max'])

        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=12)
        pdf.cell(200, 10, txt="Patient ICU Stay Summary", ln=True, align='C')
        pdf.ln(10)
        pdf.cell(200, 10, txt=f"Subject ID: {stay_results['SUBJECT_ID'].iloc[0]}", ln=True)
        pdf.cell(200, 10, txt=f"ICU Stay ID: {stay}", ln=True)
        pdf.cell(200, 10, txt=f"Diagnosis: {diagnosis}", ln=True)
        pdf.cell(200, 10, txt=f"Length of Stay (LOS): {stay_results['LOS'].iloc[0]:.2f} days", ln=True)
        pdf.cell(200, 10, txt=f"Gender: {stay_results['GENDER'].iloc[0]}", ln=True)
        pdf.cell(200, 10, txt=f"Age: {age} years", ln=True)
        pdf.ln(10)
        pdf.cell(200, 10, txt="Vital Sign Statistics:", ln=True)
        for label, row in stats.iterrows():
            text = f"{label} - Mean: {row['mean']:.2f}, Median: {row['median']:.2f}, Std: {row['std']:.2f}, Min: {row['min']:.2f}, Max: {row['max']:.2f}"
            pdf.cell(200, 10, txt=text, ln=True)
        pdf.ln(10)
        pdf.cell(200, 10, txt="Comparative Analysis with Normal Ranges:", ln=True)

        grouped_stay_results = stay_results.groupby('LABEL')
        normal_ranges = {
            "Heart Rate": (60, 100),
            "SpO2": (95, 100),
            "Temperature": (36.1, 37.2),
            "Systolic Blood Pressure": (90, 120),
            "Diastolic Blood Pressure": (60, 80),
            "Respiratory Rate": (12, 20),
            "Glucose": (70, 140)
        }


        for label, stay_values in grouped_stay_results:
            if label in normal_ranges:
                mean_value = stay_values['VALUE'].mean()
                normal_range = normal_ranges[label]
                text = f"{label} - Mean: {mean_value:.2f} (Normal Range: {normal_range[0]} - {normal_range[1]})"
                pdf.cell(200, 10, txt=text, ln=True)

        pdf.output(file_path)

async def main():
    results = await fetch_data()

    root = tk.Tk()
    app = PlotterApp(root, results)
    root.mainloop()

# Run the application
asyncio.run(main())
