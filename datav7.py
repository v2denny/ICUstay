'''
CDLE-2024-FCUP
Daniel Dias, Lucas Santiago, Miguel Lopes
ICU data visualization app
More detailed explanations are made on the report
'''

# Imports
import asyncio
import pandas as pd
from google.cloud import bigquery
import plotly.graph_objs as go
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from fpdf import FPDF
from datetime import datetime

# Initialize a BigQuery client
client = bigquery.Client()

# Define the ITEMIDs for the desired attributes (Heart Rate, O2 Percentage, Respiration Rate, etc.) 
item_ids = (211, 220045, 51, 220179, 8368, 220180, 52, 220181, 618, 220210, 646, 220277, 678, 223761, 113, 220074, 807, 220621, 40055)

# Define the SQL query
async def fetch_data(patients):
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
            t1.SUBJECT_ID IN {tuple(patients)}
            AND t1.ERROR = 0  
            AND t1.ITEMID IN {item_ids}
        ORDER BY t2.ICUSTAY_ID, t1.CHARTTIME ASC 
    """
    print("SQL Query:", query)  # Print the SQL query for verification and debugging

    query_job = client.query(query)
    results = query_job.result()

    return results.to_dataframe()


# Define a query to fetch a certain amount of distinct SUBJECT_IDs
async def fetch_subject_ids():
    query = """
        SELECT DISTINCT t1.SUBJECT_ID, t2.ICUSTAY_ID
        FROM `cdla-trabalho.CHARTEVENTS.CHARTEVENTS` AS t1
        INNER JOIN `cdla-trabalho.CHARTEVENTS.ICUSTAYS` AS t2
        ON t1.SUBJECT_ID = t2.SUBJECT_ID
        WHERE RAND() < 0.01  -- Adjusted the sampling fraction
        ORDER BY RAND()
        LIMIT 200
    """
    query_job = client.query(query)
    results = query_job.result()

    return results.to_dataframe()

# Create the app to visualize the data and analysis
class PlotterApp:
    def __init__(self, root, results, patients):
        self.root = root
        self.results = results
        print(self.results)  # Print the entire dataframe for verification and debugging
        self.patient_ids = patients
        print(f"Patient IDs: {self.patient_ids}")  # Debugging
        self.current_patient_index = 0
        self.update_stays_for_current_patient()

        self.root.title("ICU Data Plotter")

        self.top_frame = tk.Frame(self.root)
        self.top_frame.pack(side=tk.TOP, fill=tk.X)

        self.patient_frame = tk.Frame(self.top_frame)
        self.patient_frame.pack(side=tk.TOP)

        self.patient_label = tk.Label(self.patient_frame, text="Patient Operations")
        self.patient_label.pack(side=tk.LEFT, padx=5)

        self.search_bar = tk.Entry(self.patient_frame)
        self.search_bar.pack(side=tk.LEFT, padx=5)

        self.go_button = tk.Button(self.patient_frame, text="Go", command=self.search_patient)
        self.go_button.pack(side=tk.LEFT, padx=5)

        self.prev_patient_button = tk.Button(self.patient_frame, text="Previous Patient", command=self.prev_patient)
        self.prev_patient_button.pack(side=tk.LEFT, padx=5)

        self.next_patient_button = tk.Button(self.patient_frame, text="Next Patient", command=self.next_patient)
        self.next_patient_button.pack(side=tk.LEFT, padx=5)

        self.plot_frame = tk.Frame(self.root)
        self.plot_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=1)

        self.button_frame = tk.Frame(self.root)
        self.button_frame.pack(side=tk.BOTTOM, fill=tk.X)

        self.stay_button_frame = tk.Frame(self.button_frame)
        self.stay_button_frame.pack(side=tk.LEFT, expand=True)

        self.stay_label = tk.Label(self.stay_button_frame, text="Stay Operations")
        self.stay_label.pack()

        self.prev_button = tk.Button(self.stay_button_frame, text="Previous Stay", command=self.prev_stay)
        self.prev_button.pack(side=tk.LEFT, padx=5)

        self.next_button = tk.Button(self.stay_button_frame, text="Next Stay", command=self.next_stay)
        self.next_button.pack(side=tk.LEFT, padx=5)

        self.resume_button = tk.Button(self.stay_button_frame, text="Resume", command=self.show_resume)
        self.resume_button.pack(side=tk.LEFT, padx=5)

        self.compare_button = tk.Button(self.stay_button_frame, text="Comparative Analysis", command=self.show_comparative_analysis)
        self.compare_button.pack(side=tk.LEFT, padx=5)

        self.export_button = tk.Button(self.button_frame, text="Export as PDF", command=self.export_as_pdf)
        self.export_button.pack(side=tk.RIGHT, padx=5)

        self.plot_stay()

    # Show some examples of patients with stays in case of a search for invalid ID
    def print_patients_with_stays(self):
        patients_with_stays = []
        for patient_id in self.patient_ids:
            stays = self.results[self.results['SUBJECT_ID'] == patient_id]['ICUSTAY_ID'].unique()
            if len(stays) > 0:
                patients_with_stays.append(patient_id)
            if len(patients_with_stays) >= 10:
                break
        print(f"Patients with stays: {patients_with_stays}")
        messagebox.showinfo("Patients with stays", f"Patients with stays: {patients_with_stays}")

    # Implement a search bar for Patient ID's 
    def search_patient(self):
        pid = self.search_bar.get()
        try:
            pid = int(pid)
        except ValueError:
            messagebox.showerror("Invalid ID", "The patient ID is invalid. Please enter a numeric ID.")
            self.print_patients_with_stays()  # Print patients with stays if the input is invalid
            return

        print(f"Searching for patient ID: {pid}")  # Debugging
        if pid not in self.patient_ids:
            messagebox.showerror("Invalid ID", "The patient ID is not found. Please try again.")
            self.print_patients_with_stays()  # Print patients with stays if the patient ID is not found
            return

        self.current_patient_index = self.patient_ids.index(pid)
        self.update_stays_for_current_patient()
        self.plot_stay()

    # Previous patient button
    def prev_patient(self):
        if self.current_patient_index > 0:
            self.current_patient_index -= 1
            print(f"Previous patient index: {self.current_patient_index}")  # Debugging
            self.update_stays_for_current_patient()
            self.plot_stay()
        else:
            messagebox.showinfo("Info", "This is the first patient.")

    # Next patient button
    def next_patient(self):
        if self.current_patient_index < len(self.patient_ids) - 1:
            self.current_patient_index += 1
            print(f"Next patient index: {self.current_patient_index}")  # Debugging
            self.update_stays_for_current_patient()
            self.plot_stay()
        else:
            messagebox.showinfo("Info", "This is the last patient.")

    # In case of a patient with no stays, skip
    def update_stays_for_current_patient(self):
        while True:
            if self.current_patient_index >= len(self.patient_ids):
                messagebox.showinfo("Info", "No more patients with stays found.")
                self.current_patient_index = 0
                return
            current_patient = self.patient_ids[self.current_patient_index]
            print(f"Current patient ID: {current_patient}")  # Debugging
            self.stays = self.results[self.results['SUBJECT_ID'] == current_patient]['ICUSTAY_ID'].unique()
            print(f"Stays for current patient: {self.stays}")  # Debugging
            if len(self.stays) > 0:
                break
            else:
                print(f"No stays found for patient {current_patient}. Removing patient.")
                self.patient_ids.pop(self.current_patient_index)
                if self.current_patient_index >= len(self.patient_ids):
                    messagebox.showinfo("Info", "No more patients with stays found.")
                    self.current_patient_index = 0
                    return

        self.current_stay_index = 0

    # Plotting of the ICU stay
    def plot_stay(self):
        if not hasattr(self, 'stays') or len(self.stays) == 0:
            messagebox.showerror("Error", "No stays found for this patient.")
            return

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

    # Display plot
    def show_plot(self, fig):
        for widget in self.plot_frame.winfo_children():
            widget.destroy()

        canvas = fig.to_image(format="png")
        image = tk.PhotoImage(data=canvas)
        label = tk.Label(self.plot_frame, image=image)
        label.image = image
        label.pack()

    # Previous stay button
    def prev_stay(self):
        if self.current_stay_index > 0:
            self.current_stay_index -= 1
            self.plot_stay()
        else:
            messagebox.showinfo("Info", "This is the first stay.")

    # Next stay button
    def next_stay(self):
        if self.current_stay_index < len(self.stays) - 1:
            self.current_stay_index += 1
            self.plot_stay()
        else:
            messagebox.showinfo("Info", "This is the last stay.")

    # Show a resume of the stay
    def show_resume(self):
        stay = self.stays[self.current_stay_index]
        stay_results = self.results[self.results['ICUSTAY_ID'] == stay]
        diagnosis = stay_results['DIAGNOSIS'].iloc[0] if 'DIAGNOSIS' in stay_results.columns else "No diagnosis info"

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

    # Show a comparative analysis between values of the stay and standard values
    def show_comparative_analysis(self):
        stay = self.stays[self.current_stay_index]
        stay_results = self.results[self.results['ICUSTAY_ID'] == stay]

        comparison_window = tk.Toplevel(self.root)
        comparison_window.title("Comparative Analysis")

        normal_ranges = {
            "Heart Rate": (60, 100),
            "O2 saturation pulseoxymetry": (95, 100),
            "Temperature Fahrenheit": (96.9, 98.9),
            "Non Invasive Blood Pressure systolic": (90, 120),
            "Non Invasive Blood Pressure diastolic": (60, 80),
            "Respiratory Rate": (12, 20),
            "Glucose (serum)": (70, 140)
        }

        grouped_stay_results = stay_results.groupby('LABEL')

        ttk.Label(comparison_window, text="Comparative Analysis with Normal Ranges:").pack(pady=10)
        for label, stay_values in grouped_stay_results:
            if label in normal_ranges:
                mean_value = stay_values['VALUE'].mean()
                normal_range = normal_ranges[label]
                text = f"{label} - Mean: {mean_value:.2f} (Normal Range: {normal_range[0]} - {normal_range[1]})"
                ttk.Label(comparison_window, text=text).pack(pady=2)

    # Export as PDF button
    def export_as_pdf(self):
        file_path = filedialog.asksaveasfilename(defaultextension=".pdf", filetypes=[("PDF files", "*.pdf")])
        if not file_path:
            return

        stay = self.stays[self.current_stay_index]
        stay_results = self.results[self.results['ICUSTAY_ID'] == stay]
        diagnosis = stay_results['DIAGNOSIS'].iloc[0] if 'DIAGNOSIS' in stay_results.columns else "No diagnosis info"

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

# Main
async def main():
    patients = await fetch_subject_ids()
    print(patients.head(100))
    patients = list(patients['SUBJECT_ID'].unique())
    print(patients)
    results = await fetch_data(patients)
    print(results.head())  # Debugging print


    root = tk.Tk()
    app = PlotterApp(root, results, patients)
    root.mainloop()

# Run the application
asyncio.run(main())
