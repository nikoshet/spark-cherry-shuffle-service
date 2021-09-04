from __future__ import print_function
import os
import sys
import PySimpleGUI as sg

if __name__ == '__main__':
       import PySimpleGUI as sg

       sg.theme('LightGrey1')
       #sg.theme_previewer()


       # First the window layout...2 columns
       left_col = [[sg.Text('CLUSTER SETUP', font=('Helvetica', 14), justification='left')],
                  [sg.Text('Select Number of Spark Workers', font=('Helvetica', 12), justification='left')],
                  [sg.Radio('2', 'workers', default=True, size=(4, 1)), sg.Radio('4', 'workers', size=(4, 1)), sg.Radio('8', 'workers', size=(4, 1)), sg.Radio('16', 'workers', size=(4, 1))],
                  [sg.Text('Select Spark Type', font=('Helvetica', 12), justification='left')],
                  [sg.Radio('Spark w/ CHERRY', 'type', default=True, size=(16, 1)), sg.Radio('Vanilla Spark', 'type', size=(16, 1)), sg.Radio('Spark w/ ESS', 'type', size=(22, 1))],
                  [sg.Button('Create Spark Cluster')]]

       right_col = [[sg.Text('WORKLOAD SETUP', font=('Helvetica', 14), justification='left')],
                    [sg.Text('Select Number of Map/Reduce Tasks', font=('Helvetica', 12), justification='left')],
                    [sg.Radio('10', 'tasks', default=True, size=(4, 1)), sg.Radio('200', 'tasks', default=True, size=(4, 1)), sg.Radio('700', 'tasks', size=(4, 1)), sg.Radio('1500', 'tasks', size=(4, 1))],
                    [sg.Text('Select Size of Dataset', font=('Helvetica', 12), justification='left')],
                    [sg.Radio('1GB', 'size', default=True, size=(4, 1)), sg.Radio('5GB', 'size', size=(4, 1)), sg.Radio('10GB', 'size', size=(4, 1))],
                    [sg.Text('Select Workload Type', font=('Helvetica', 12), justification='left')],
                    [sg.Radio('Synthetic Workload', 'workload', default=True, size=(22, 1)), sg.Radio('TPC-DS', 'workload', size=(8, 1)), sg.Text('Query Selection (1-99)', size=(22, 1)), sg.Spin(values=[i for i in range(1, 99)], initial_value=1, size=(3, 1))],
                    [sg.Button('Start Workload')]]

       #bottom_left_col = [[sg.Text('Script output', size=(30, 1))],[sg.Output(size=(51, 14), font='Courier 11')]]
       #bottom_right_col = [[sg.Text('Manual command', size=(15, 1))], [sg.Input(focus=True, key='-IN-'), sg.Button('Run', bind_return_key=True)]]

       # ----- Full layout -----
       layout = [[sg.Column(left_col, element_justification='c'), sg.VSeperator(), sg.Column(right_col, element_justification='c')],
           [sg.Text('_'  * 141)],
           #[sg.Column(bottom_left_col), sg.Column(bottom_right_col)],
           [sg.Text('Script output', size=(30, 1))],
           [sg.Output(size=(108, 10), font='Courier 11')],
           [sg.Text('Manual command', size=(15, 1)), sg.Input(focus=True, key='-IN-'), sg.Button('Run', bind_return_key=True)],
           [sg.Button('EXIT')] #,sg.T(' ' * 58),
           #[sg.Button('script1'), sg.Button('script2')],
           #[sg.Button('EXIT')]
       ]

       window = sg.Window('CHERRY Control Panel', layout)

       # ---===--- Loop taking in user input and using it to call scripts --- #

       while True:
           event, values = window.read()
           if event == 'EXIT'  or event == sg.WIN_CLOSED:
               break # exit button clicked
           if event == 'script1':
               sp = sg.execute_command_subprocess('pip', 'list', wait=True)
               print(sg.execute_get_results(sp)[0])
           elif event == 'script2':
               print('Running python --version')
               # For this one we need to wait for the subprocess to complete to get the results
               sp = sg.execute_command_subprocess('python', '--version', wait=True)
               print(sg.execute_get_results(sp)[0])
           elif event == 'Run':
               args = values['-IN-'].split(' ')
               print('Running {values["-IN-"]} args={args}')
               sp = sg.execute_command_subprocess(args[0], *args[1:])
               # This will cause the program to wait for the subprocess to finish
               print(sg.execute_get_results(sp)[0])
           elif event == 'Run No Wait':
               args = values['-IN-'].split(' ')
               print('Running {values["-IN-"]} args={args}', 'Results will not be shown')
               sp = sg.execute_command_subprocess(args[0], *args[1:])
           elif event == 'Create Spark Cluster':
               print('\nStarting Spark cluster with Kubernetes...... \nCluster is ready.    - Spark Setup: w/ CHERRY.    - Number of  Spark Workers running: 4')
           elif event == 'Start Workload':
               print('\nStarting Workload...... \nWorkload started.    - Number of Map/Reduce tasks: 1500.    - Shuffle Block Size: 2.2 KB.    - Dataset Size: 5GB.    - Workload type: Synthetic workload.')