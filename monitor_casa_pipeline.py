#!/usr/bin/env python3
"""
CASA Pipeline Monitor - Simple Demo for Portafolio A Cancino
Monitor 4 basic tasks of CASA with Prometheus
"""

# TO DO: 
# Clean up comments
#  quote CASA-PIPELINE NRAO docs for the tools. Being honest, im kinda running blind in the dark with this

import os
import time
import psutil
from pathlib import Path
from prometheus_client import start_http_server, Gauge, Counter

# Config
PROMETHEUS_PORT = 9091
WORK_DIR = Path.home() / "casa_demo"
IMAGE_SIZE = 512  
ITERATIONS = 200  
NUM_CHANNELS = 50 

# prometheus
task_time = Gauge('casa_task_seconds', 'Tiempo de ejecución', ['task'])
cpu_percent = Gauge('casa_cpu_percent', 'Uso de CPU')
memory_mb = Gauge('casa_memory_mb', 'Memoria usada (MB)')
tasks_ok = Counter('casa_tasks_ok', 'Tasks exitosas')
tasks_fail = Counter('casa_tasks_fail', 'Tasks fallidas')
pipeline_ok = Gauge('casa_pipeline_ok', 'Pipeline OK (1) o Error (0)')

#helprs
def update_resources():
    """ UPDATE CPU & MEM Usage"""
    cpu_percent.set(psutil.cpu_percent(interval=0.1))
    memory_mb.set(psutil.Process().memory_info().rss / (1024**2))


def run_task(name, func):
    """Run Task and keep time"""
    print(f"▶ {name}...", end=" ", flush=True)
    update_resources()
    
    start = time.time()
    try:
        func()
        elapsed = time.time() - start
        task_time.labels(task=name).set(elapsed)
        tasks_ok.inc()
        print(f"✓ ({elapsed:.2f}s)")
        return True
    except Exception as e:
        elapsed = time.time() - start
        task_time.labels(task=name).set(elapsed)
        tasks_fail.inc()
        print(f"✗ Error: {e}")
        return False

#tareas para exigir el uso del equipo 
def task1_tclean():
    """Imaging with tclean"""
    from casatools import image
    
    img = str(WORK_DIR / "img1.image")
    ia = image()
    ia.fromshape(img, [IMAGE_SIZE, IMAGE_SIZE, 1, NUM_CHANNELS])
    ia.close()
    print(f"[imagen {IMAGE_SIZE}x{IMAGE_SIZE}x{NUM_CHANNELS}]", end=" ")

def task2_immath():
    """Math imgs"""
    from casatasks import immath
    from casatools import image
    import numpy as np
    
    images = []
    for i in range(3):
        img = str(WORK_DIR / f"math{i}.image")
        ia = image()
        ia.fromshape(img, [IMAGE_SIZE, IMAGE_SIZE, 1, 1])
        
        ###############datos
        data = np.random.randn(IMAGE_SIZE, IMAGE_SIZE).astype('float32')
        ia.putchunk(data)
        ia.close()
        images.append(img)
    
    out = str(WORK_DIR / "result.image")
    
    # gracias ian por la ayuda 
    immath(
        imagename=images,
        mode='evalexpr',
        expr='(IM0+IM1)*IM2',
        outfile=out
    )


def task3_imstat():
    """IMG Stadistics"""
    from casatasks import imstat
    from casatools import image
    import numpy as np
    
    img = str(WORK_DIR / "stats.image")
    
    ia = image()
    ia.fromshape(img, [IMAGE_SIZE, IMAGE_SIZE, 1, NUM_CHANNELS])
    
    for chan in range(NUM_CHANNELS):
        data = np.random.randn(IMAGE_SIZE, IMAGE_SIZE).astype('float32')
        ia.putchunk(data, blc=[0, 0, 0, chan])
    
    ia.close()
    
    stats = imstat(
        imagename=img,
        axes=[0, 1],
        algorithm='chauvenet'  
    )
    print(f"[{NUM_CHANNELS} canales]", end=" ")


def task4_exportfits():
    """Export to FITS"""
    from casatasks import exportfits
    from casatools import image
    
    img = str(WORK_DIR / "export.image")
    fits = str(WORK_DIR / "output.fits")
    
    ia = image()
    ia.fromshape(img, [IMAGE_SIZE, IMAGE_SIZE, 1, 1])
    ia.close()
    
    exportfits(imagename=img, fitsimage=fits, overwrite=True)
    
    size = os.path.getsize(fits) / (1024**2)
    print(f"[{size:.2f}MB]", end=" ")


###############################

def main():
    print("\n" + "="*60)
    print("CASA PIPELINE MONITOR")
    print("="*60)
    
    WORK_DIR.mkdir(exist_ok=True)
    import shutil
    if WORK_DIR.exists():
        shutil.rmtree(WORK_DIR)
    WORK_DIR.mkdir()

    #### en caso de no tener el servicio, por que no    
    start_http_server(PROMETHEUS_PORT)
    print(f"📊 Métrics: http://localhost:{PROMETHEUS_PORT}/metrics\n")
    
    # Verificar CASA. PD: Por temas de empaquetado, me reducee timepo prueba y error 
    try:
        from casatasks import tclean
    except ImportError:
        print("❌ Ejecuta con: casa -c script.py")
        return
    
    # pipeline
    print("Runing Taks :")
    start = time.time()
    
    results = [
        run_task("1.tclean", task1_tclean),
        run_task("2.immath", task2_immath),
        run_task("3.imstat", task3_imstat),
        run_task("4.exportfits", task4_exportfits),
    ]
    
    total = time.time() - start
    
    ######################### resultado
    ok = sum(results)
    pipeline_ok.set(1 if ok == 4 else 0)
    
    print(f"\n{'='*60}")
    print(f"Resultado: {ok}/4 OK en {total:.2f}s")
    print(f"CPU: {psutil.cpu_percent():.1f}% | RAM: {psutil.Process().memory_info().rss/(1024**2):.0f}MB")
    print(f"{'='*60}\n")
    
    #### wanna keep
    print("⏸  Running. Ctrl+C to exit...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n👋 STOPPED")
        
        ### after care (necesito mas espacio :c)
        resp = input("¿Delete temps? (s/N): ")
        if resp.lower() == 'y':
            os.system(f"rm -rf {WORK_DIR}")
            print("✓ Limpieza completada")


if __name__ == "__main__":
    main()