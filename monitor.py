from aswan import Project
from aswan.monitor_app import MonitorApp

project = Project(name="ingatlan_sale")
MonitorApp(project.depot, refresh_interval_secs=10).app.run_server(debug = True)