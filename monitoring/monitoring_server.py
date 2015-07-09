from flask import Flask, render_template
app = Flask(__name__)

import json
import os
import traceback
ROOT_DIR = os.path.expanduser('~') + '/.dispel4py/monitoring'


@app.route('/monitoring/<job>')
def show_status(job):
    try:
        status_file = os.path.join(ROOT_DIR, job)
        with open(status_file, 'r') as f:
            job_status = json.load(f)
        print(job_status)
        return render_template("job_status.html", job=job_status)
    except:
        print(traceback.format_exc())
        raise


@app.route('/monitoring')
def list_jobs():
    jobs = []
    try:
        for fn in os.listdir(ROOT_DIR):
            filename = os.path.join(ROOT_DIR, fn)
            if os.path.isfile(filename):
                with open(filename, 'r') as f:
                    job_status = json.load(f)
                    jobs.append({'name': job_status['name'],
                                 'start_time': job_status['start_time']})
    except:
        # the directory might not exist but that's ok
        pass

    return render_template("index.html", job_list=jobs)


if __name__ == "__main__":
    app.run(debug=True)
