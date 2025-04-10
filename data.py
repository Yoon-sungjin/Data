import os
import shutil
import pandas as pd
import time
import json
import pydicom

class OrganizeMedicalImgaingInfo:
    def __init__(self, reference_patient_excel_dirs, source_dir, destination_dir):
        self.reference_patient_excel_dirs = reference_patient_excel_dirs
        self.destination_dir = destination_dir
        self.source_dir = source_dir
        self.log_file = "log.log"

    def log(self, message):
        with open(self.log_file, "a") as f:
            f.write(message + "\n")



    def make_json(self, medimg_analysis_excel_path, patientID, destination_patient_folder_path):
        json_filename = patientID + ".json"
        json_file_path = os.path.join(destination_patient_folder_path, json_filename)

        # Check if JSON file already exists
        if os.path.exists(json_file_path):
            # If JSON file exists, load existing data
            with open(json_file_path, 'r', encoding='utf-8') as json_file:
                existing_data = json.load(json_file)
        else:
            # If JSON file doesn't exist, create empty data structure
            existing_data = {
                "PatientID": patientID,
                "PatientName": "",
                "Sex": "",
                "Examinations": []
            }

        medimg_analysis_excel_df = pd.read_excel(medimg_analysis_excel_path, header=0)
        medimg_analysis_excel_df = medimg_analysis_excel_df.drop(["검사나이", "나이", "검날날짜"], axis=1)
        medimg_analysis_excel_df = medimg_analysis_excel_df.rename(
            columns={
                "검사번호": "ExamNumber",
                "환자번호": "PatientID",
                "환자명": "PatientName",
                "검사코드": "ExamCode",
                "검사명": "ExamName",
                "Modality": "Modality",
                "Machine Name": "MachineName",
                "검사일자시간": "ExamDateTime",
                "성별": "Gender",
                "CONCLUSION": "Conclusion",
                "FINDING": "Finding",
            }
        )
        # Fill NaN values with "None"
        medimg_analysis_excel_df.fillna("None", inplace=True)
        # etc data to string
        medimg_analysis_excel_df["ExamNumber"] = medimg_analysis_excel_df["ExamNumber"].apply(str)
        medimg_analysis_excel_df["PatientID"] = medimg_analysis_excel_df["PatientID"].apply(str)
        medimg_analysis_excel_df["ExamDateTime"] = medimg_analysis_excel_df["ExamDateTime"].apply(str)

        filtered_df = medimg_analysis_excel_df.loc[medimg_analysis_excel_df['PatientID'] == patientID]

        # Check if patient information exists in the dataframe
        if not filtered_df.empty:
            # Update patient information if necessary
            existing_data["PatientName"] = filtered_df.iloc[0]['PatientName']
            existing_data["Sex"] = filtered_df.iloc[0]['Gender']

            # Iterate over each examination
            for index, row in filtered_df.iterrows():
                # Check if examination already exists
                exam_exists = any(
                    exam['ExamDateTime'] == row['ExamDateTime'] and
                    exam['Modality'] == row['Modality'] and
                    exam['ExamName'] == row['ExamName']
                    for exam in existing_data["Examinations"]
                )
                if not exam_exists:
                    # Construct the folder name based on ExamDateTime, Modality, and ExamName
                    folder_name = f"{row['ExamDateTime'][:10].replace('-', '')}_{row['Modality']}_{row['ExamName']}"
                    # Get the relative path of the folder with the constructed folder name
                    relative_folder_path = os.path.relpath(os.path.join(destination_patient_folder_path, folder_name), os.path.dirname(json_file_path))

                    try:
                        # Open a DICOM file to extract StudyInstanceUID and Modality
                        dicom_files = os.listdir(os.path.join(destination_patient_folder_path, relative_folder_path))
                        
                        if dicom_files:
                            sample_dicom_file = dicom_files[0]  # Take any DICOM file as a sample
                            dicom_file_path = os.path.join(os.path.join(destination_patient_folder_path, relative_folder_path), sample_dicom_file)
                            # Read the DICOM file
                            ds = pydicom.dcmread(dicom_file_path)
                            # Extract StudyInstanceUID and Modality
                            study_instance_uid = ds.get("StudyInstanceUID", "")
                            modality = ds.get("Modality", "")
                            
                            # Check if Modality from DICOM matches Modality from Excel
                            if not modality == row['Modality']:
                                study_instance_uid = ""
                                print(f"Modality from DICOM '{modality}' does not match Modality from Excel '{row['Modality']}' for '{row['ExamName']}' examination of patient '{patientID}'.")
                            
                            examination_info = {
                                    "ExamDateTime": row['ExamDateTime'],
                                    "Modality": row['Modality'],
                                    "ExamName": row['ExamName'],
                                    "ExamCode": row['ExamCode'],
                                    "MachineName": row['MachineName'],
                                    "Conclusion": row['Conclusion'],
                                    "Finding": row['Finding'],
                                    "Location": relative_folder_path,
                                    "StudyInstanceUID": study_instance_uid
                                }
                            existing_data["Examinations"].append(examination_info)
                            
                            print(f"We make '{patientID}' json file, date is {row['ExamDateTime']}")
                                
                        else:
                            print(f"No DICOM files found for '{row['ExamName']}' examination of patient '{patientID}'.")
                            self.log(f"No DICOM files found for '{row['ExamName']}' examination of patient '{patientID}'.")
                            
                    except Exception as e:
                        print(f"Error occurred while creating json file for '{folder_name}' of patient '{patientID}'. Error: {str(e)}.")
                        self.log(f"Error occurred while creating json file for '{folder_name}' of patient '{patientID}'. Error: {str(e)}.\nCheck whether there is a match between the analysis excel data and the dcm file name.")

                else: 
                    print(f"We skip making '{patientID}' json file, date is {row['ExamDateTime']}")

            # Write updated JSON to file
            with open(json_file_path, 'w', encoding='utf-8') as json_file:
                json.dump(existing_data, json_file, indent=4, ensure_ascii=False)
        else:
            print(f"!No examination data found for patient {patientID}. JSON file will not be updated.")




    def make_patient_folders(self):
        # self.delete_previous_json()
        for type_of_cancer in self.reference_patient_excel_dirs:
            df = pd.read_excel(self.reference_patient_excel_dirs[type_of_cancer], header=1)
            order_folders = [dir_name for dir_name in os.listdir(self.source_dir) if os.path.isdir(os.path.join(self.source_dir, dir_name))]
            xlsx_file = next((file_name for file_name in os.listdir(self.source_dir) if file_name.endswith(('.xlsx', '.csv'))), None)
            xlsx_file_path = os.path.join(self.source_dir, xlsx_file)
            xlsx_file_df = pd.read_excel(xlsx_file_path, header=0)

            for order_folder in order_folders:
                order_folder_path = os.path.join(self.source_dir, order_folder)
                nth_order_patient_folders = os.listdir(order_folder_path)
                
                for nth_order_patient_folder in nth_order_patient_folders:
                    nth_order_patient_folder_path = os.path.join(order_folder_path, nth_order_patient_folder)
                    if os.path.isdir(nth_order_patient_folder_path):

                        patient_folder_parts = nth_order_patient_folder.split("_")
                        patient_ID = patient_folder_parts[1]
                        patient_examination_date = patient_folder_parts[2]
                        patient_examination_number = patient_folder_parts[0]
                        patient_examination_name = xlsx_file_df.loc[xlsx_file_df["검사번호"] == int(patient_examination_number), "검사명"].iloc[0]

                        # 특정 암종의 DataFrame에 있는 환자 번호와 폴더 이름이 일치하는 경우에만 진행
                        if int(patient_ID) in df["ID"].values:
                            
                            dicom_files = os.listdir(nth_order_patient_folder_path)
                            if dicom_files:
                                sample_dicom_file = dicom_files[0]  # Take any DICOM file as a sample
                                dicom_file_path = os.path.join(nth_order_patient_folder_path, sample_dicom_file)
                                # Read the DICOM file
                                ds = pydicom.dcmread(dicom_file_path)
                                # Extract StudyInstanceUID and Modality
                                modality = ds.get("Modality", "")
                                nth_order_dcm_folder = f"{patient_examination_date}_{modality}_{patient_examination_name}"
                            else:
                                nth_order_dcm_folder = f"{patient_examination_date}_{patient_examination_name}"
                            nth_order_dcm_folder_path = nth_order_patient_folder_path
                            destination_dcm_folder_path = os.path.join(self.destination_dir, type_of_cancer, patient_ID, nth_order_dcm_folder)

                            try:
                                start_time = time.time()
                                shutil.copytree(nth_order_dcm_folder_path, destination_dcm_folder_path)
                                end_time = time.time()
                                execution_time = end_time - start_time
                                print(f">>> Execution time of file '{nth_order_dcm_folder}': {execution_time} seconds")
                                print(f"Cancer type '{type_of_cancer}', Patient ID: '{patient_ID}', file '{nth_order_dcm_folder}'\t copied successfully.")
                            
                            except FileExistsError:
                                print(f"Cancer type '{type_of_cancer}', Patient ID: '{patient_ID}', file '{nth_order_dcm_folder}'\t already exists. Skipping...")
                            
                            except FileNotFoundError:
                                print(f"Cancer type '{type_of_cancer}', Patient ID: '{patient_ID}', file '{nth_order_dcm_folder}'\t not found.")
                                self.log(f"Cancer type '{type_of_cancer}', Patient ID: '{patient_ID}', file '{nth_order_dcm_folder}'\t not found.")

                            #json 파일 만들기
                            destination_patient_folder_path = os.path.join(self.destination_dir, type_of_cancer, patient_ID)
                            self.make_json(xlsx_file_path, patient_ID, destination_patient_folder_path)
                        
                            
                            

if __name__ == "__main__":
    reference_patient_excel_dirs = {"breast": "\\\\snuampl\\SNUICT\\Clinical\\breast\\SNUH_breast_clinical_include.xlsx", 
                                    "headandneck": "\\\\snuampl\\SNUICT\\Clinical\\headandneck\\SNUH_headandneck_clinical_include.xlsx", 
                                    "lung": "\\\\snuampl\\SNUICT\\Clinical\\lung\\SNUH_lung_clinical_include.xlsx",
                                    "meta": "\\\\snuampl\\SNUICT\\Clinical\\meta\\SNUH_meta_clinical_include.xlsx",
                                    "prostate": "\\\\snuampl\\SNUICT\\Clinical\\prostate\\SNUH_prostate_clinical_include.xlsx"}
    
    source_dir = "\\\\snuampl\\SNUICT\\Radiology\\raw\\test\\test2"
    destination_dir = "\\\\snuampl\\SNUICT\\Radiology\\result2"

    organizeMedicalImgaingInfo = OrganizeMedicalImgaingInfo(reference_patient_excel_dirs, source_dir, destination_dir)

    with open(organizeMedicalImgaingInfo.log_file, "w") as f:
        f.write("Log File\n\n")

    entire_start_time = time.time()
    organizeMedicalImgaingInfo.make_patient_folders()
    entire_end_time = time.time()
    entire_execution_time = entire_end_time - entire_start_time
    print(f"Entire execution time: {entire_execution_time} seconds")
    organizeMedicalImgaingInfo.log(f"Entire execution time: {entire_execution_time} seconds")