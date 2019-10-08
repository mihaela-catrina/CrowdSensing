"""
This module represents a device.

Computer Systems Architecture Course
Assignment 1
March 2019
"""

from threading import Thread, Lock, Semaphore, Event
from Queue import Queue


class ReusableBarrierDevices(object):
    """
    Class that represents a reusable barrier -> from lab.
    """

    def __init__(self, num_threads):
        self.num_threads = num_threads
        self.count_threads1 = [self.num_threads]
        self.count_threads2 = [self.num_threads]
        self.count_lock = Lock()
        self.threads_sem1 = Semaphore(0)
        self.threads_sem2 = Semaphore(0)

    def wait(self):
        """
        wait method for barrier
        """
        self.phase(self.count_threads1, self.threads_sem1)
        self.phase(self.count_threads2, self.threads_sem2)

    def phase(self, count_threads, threads_sem):
        """
        wait method helper
        """
        with self.count_lock:
            count_threads[0] -= 1
            if count_threads[0] == 0:
                for _ in range(self.num_threads):
                    threads_sem.release()
                count_threads[0] = self.num_threads
        threads_sem.acquire()


class Device(object):
    """
    Class that represents a device.
    """

    def __init__(self, device_id, sensor_data, supervisor):

        """
        Constructor.

        @type device_id: Integer
        @param device_id: the unique id of this node; between 0 and N-1

        @type sensor_data: List of (Integer, Float)
        @param sensor_data: a list containing (location, data) as
        measured by this device

        @type supervisor: Supervisor
        @param supervisor: the testing infrastructure's control and
        validation component
        """
        self.device_id = device_id
        self.sensor_data = sensor_data
        self.supervisor = supervisor
        self.scripts = Queue()
        self.scripts_list = []
        self.timepoint_done = Event()
        self.thread = DeviceThread(self)
        self.barrier = None

        self.neighbours = None
        self.lock_dict = {location: Lock() for location in sensor_data}

        self.thread.start()

    def __str__(self):
        """
        Pretty prints this device.

        @rtype: String
        @return: a string containing the id of this device
        """
        return "Device %d" % self.device_id

    def setup_devices(self, devices):
        """
        Setup the devices before simulation begins.

        @type devices: List of Device
        @param devices: list containing all devices
        """

        # first device will make all the required setup
        if self.device_id == 0:
            barrier = ReusableBarrierDevices(len(devices))
            for dev in devices:
                dev.barrier = barrier

    def assign_script(self, script, location):
        """
        Provide a script for the device to execute.

        @type script: Script
        @param script: the script to execute from now on at each
         timepoint; None if the
            current timepoint has ended

        @type location: Integer
        @param location: the location for which the script is
         interested in
        """
        if script is not None:
            self.scripts_list.append((script, location))
        else:
            self.timepoint_done.set()

    def get_data(self, location):
        """
        Returns the pollution value this device has for the
         given location.

        @type location: Integer
        @param location: a location for which obtain the
         data

        @rtype: Float
        @return: the pollution value
        """
        # atomic get -> acquire location
        if location in self.sensor_data:
            self.lock_dict[location].acquire()
            return self.sensor_data[location]

        return None

    def set_data(self, location, data):
        """
        Sets the pollution value stored by this device for the given location.

        @type location: Integer
        @param location: a location for which to set the data

        @type data: Float
        @param data: the pollution value
        """
        # when data is set we can call release for the location
        if location in self.sensor_data:
            self.sensor_data[location] = data
            self.lock_dict[location].release()

    def shutdown(self):
        """
        Instructs the device to shutdown (terminate all threads).
         This method
        is invoked by the tester. This method must block until all
         the threads
        started by this device terminate.
        """

        self.thread.join()


class DeviceThread(Thread):
    """
    Class that implements the device's worker thread.
    """

    def __init__(self, parent):
        """
        Constructor.

        @type parent: Device
        @param parent: the device which owns this thread
        """
        Thread.__init__(self)
        self.device = parent
        self.cores = []
        self.neighbours = None

    def run(self):
        for _ in range(8):
            self.cores.append(CoreThread(self))
        for worker in self.cores:
            worker.start()

        while True:
            self.neighbours = self.device.supervisor.get_neighbours()
            if self.neighbours is None:
                break

            self.device.timepoint_done.wait()

            for scr in self.device.scripts_list:
                self.device.scripts.put(scr)

            self.device.scripts.join()
            self.device.timepoint_done.clear()
            self.device.barrier.wait()

        # end core threads
        for _ in self.cores:
            self.device.scripts.put(None)

        for core in self.cores:
            core.join()


class CoreThread(Thread):
    """
    Constructor.

    @type parent: DeviceThread
    @param parent: the device which owns this core
    """
    def __init__(self, parent):
        Thread.__init__(self)
        self.parent = parent

    def run(self):

        while True:
            pair = self.parent.device.scripts.get()
            if pair is None:
                self.parent.device.scripts.task_done()
                break

            location_for_script = pair[1]
            script = pair[0]

            script_data = []
            # collect data from current neighbours
            for device in self.parent.neighbours:
                if device.device_id != self.parent.device.device_id:
                    data = device.get_data(location_for_script)
                    if data is not None:
                        script_data.append(data)

            # add our data, if any
            data = self.parent.device.get_data(location_for_script)
            if data is not None:
                script_data.append(data)

            if script_data:
                # run script on data
                result = script.run(script_data)

                # update data of neighbours
                for device in self.parent.neighbours:
                    if device.device_id != self.parent.device.device_id:
                        device.set_data(location_for_script, result)

                # update our data
                self.parent.device.set_data(location_for_script, result)
            # notify task completion
            self.parent.device.scripts.task_done()
