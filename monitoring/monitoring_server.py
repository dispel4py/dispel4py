from flask import Flask, \
    render_template, send_from_directory, abort, redirect, url_for
app = Flask(__name__)

import json
import os
import traceback
ROOT_DIR = os.path.expanduser('~') + '/.dispel4py/monitoring'


@app.route('/monitoring/<job>/graph')
def show_graph(job):
    try:
        job_dir = os.path.join(ROOT_DIR, job)
        return send_from_directory(job_dir, 'graph.png', mimetype='image/png')
    except:
        abort(404)


@app.route('/monitoring/<job>/summary')
def show_status(job):
    try:
        job_dir = os.path.join(ROOT_DIR, job)
        with open(os.path.join(job_dir, 'stack'), 'r') as f:
            job_status = json.load(f)
        # print(job_status)
        return render_template("job_status.html", job=job_status)
    except:
        print(traceback.format_exc())
        raise


@app.route('/monitoring/<job>/timeline')
def serve_timeline(job):
    try:
        info_path = os.path.join(ROOT_DIR, job + '/info')
        with open(info_path, 'r') as f:
            info = json.load(f)
        timeline_path = os.path.join(
            ROOT_DIR, job + '/timestamps')
        with open(timeline_path, 'r') as f:
            from collections import deque
            result = []
            for line in deque(f, maxlen=100):
                try:
                    result.append(json.loads(line))
                except ValueError:
                    # some JSON error
                    pass
        #     return f.read()
        return render_template(
            "job_timeline.html",
            info=json.dumps(info),
            timeline=json.dumps(result))
    except:
        print(traceback.format_exc())
        raise


@app.route('/monitoring/<job>/info')
def show_job_info(job):
    job_dir = os.path.join(ROOT_DIR, job)
    if os.path.isdir(job_dir):
        with open(os.path.join(job_dir, 'info'), 'r') as f:
            job_info = json.load(f)
        processes = []
        for procs in job_info['processes'].values():
            processes.extend(procs)
        job_info['num_processes'] = len(processes)
        if os.path.isfile(os.path.join(job_dir, 'stack')):
            job_info['has_summary'] = True
        if os.path.isfile(os.path.join(job_dir, 'timestamps')):
            job_info['has_timeline'] = True
        return render_template("job_info.html", job=job_info)


@app.route('/monitoring/<job>/delete', methods=['POST'])
def delete_stats(job):
    import shutil
    print('Deleting %s' % job)
    job_dir = os.path.join(ROOT_DIR, job)
    shutil.rmtree(job_dir, True)
    return redirect(url_for('list_jobs'), code=302)


@app.route('/monitoring')
def list_jobs():
    jobs = []
    try:
        print(os.listdir(ROOT_DIR))
        for fn in os.listdir(ROOT_DIR):
            filename = os.path.join(ROOT_DIR, fn)
            if os.path.isdir(filename):
                with open(os.path.join(filename, 'info'), 'r') as f:
                    job_status = json.load(f)
                    job = {'name': job_status['name'],
                           'start_time': job_status['start_time']}
                    if 'end_time' in job_status:
                        job['end_time'] = job_status['end_time']
                jobs.append(job)
    except:
        print(traceback.format_exc())
        # the directory might not exist yet but that's ok
        pass

    return render_template("index.html", job_list=jobs)


if __name__ == "__main__":
    app.run(debug=True)
