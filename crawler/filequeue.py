import pickle
import os
import threading
import weakref

__author__ = 'roelvdberg@gmail.com'


def _file_method(method, pickled=False):
    return method + 'b' if pickled else method


def _touch(file_name, pickled=False, keep=False):
    method = _file_method('a', pickled) if keep else \
        _file_method('w', pickled)
    with open(file_name, method):
        os.utime(file_name, None)


def del_file(filename):
    try:
        os.remove(filename)
    except FileNotFoundError:
        pass


def _remove(put_queue_name, get_queue_name, pos_name):
    del_file(pos_name)
    del_file(put_queue_name)
    del_file(get_queue_name)


def _remove_persistent(put_queue_name, get_queue_name, pickled, pos_name):
    try:
        try:
            with open(pos_name, 'r') as pn:
                get_pos = int(pn.read())
        except FileNotFoundError:
            get_pos = 0
        os.remove(pos_name)
        method = 'ab' if pickled else 'a'
        with open(put_queue_name, method) as put_queue, \
                open(get_queue_name, _file_method('r', pickled)) as f:
            if pickled:
                for _ in range(get_pos):
                    pickle.load(f)
                while True:
                    try:
                        pickle.dump(pickle.load(f), put_queue)
                    except EOFError:
                        break
            else:
                for _ in range(get_pos):
                    next(f)
                for line in f:
                    put_queue.write(line.strip('\n') + '\n')
        _touch(get_queue_name, pickled=pickled)
    except (Empty, FileNotFoundError):
        pass


class FileQueueError(Exception):
    pass


class Empty(Exception):
    pass


class FileQueue(object):
    """
    Low memory FIFO* queue that keeps queue on disk instead of in memory.

    * FIFO: First In First Out. When a queue is made persistent and reused,
    this FIFO is broken in a sense that when a queue is deleted (but the
    files still persist) the items still in the get file are put into the put
    file and thus these items in front of the queue are put in the back.
    """

    def __init__(self, directory="", name=None, persistent=False,
                 overwrite=False, id_=0, pickled=True):
        """
        Low memory FIFO queue that keeps queue on disk.

        Queue is stored in three files:
        'get_thread_[thread-id or given name]_[id].queue'
        'put_thread_[thread-id or given name]_[id].queue'
        'pos_thread_[thread-id or given name]_[id].queue'

        The former (read) is used for reading the queue and gets filled with the
        latter (put) when empty. The latter (put) is used to put new items into
        the queue.

        :param directory: Directory where the queue files are stored.
        :param name: Base name of the files. Default: Thread id.
        :param persistent: When True files are not removed on shutdown or
            deletion. Default: False.
        :param overwrite: When True uses files that are allready stored on disk
            (with persistence is True) or overwrites them (with persistence is
            False). Default: False
        :param id_: id of the file, when overwrite is True this is increased
            when file allready exists with that id until an unused id is
            found. Default: 0
        :param pickled: uses pickle by default to serialize the items. When
            the items are strings only, pickled can be set to False.
        """
        self.fq = _PersistentFileQueue(
                directory, name, persistent, overwrite, id_, pickled
            )
        if persistent:
            self._finalizer = weakref.finalize(
                self, _remove_persistent, self.fq.put_queue_name,
                self.fq.get_queue_name, pickled, self.fq.pos_name
            )
        else:
            self._finalizer = weakref.finalize(
                self, _remove, self.fq.put_queue_name, self.fq.get_queue_name,
                self.fq.pos_name
            )

    def put(self, x):
        """
        Put item into the queue.

        :param item: string or other Python object to put in queue.
        """
        self.fq.put(x)

    def get(self):
        """
        Remove and return an item from the queue.

        Raises Empty when empty.

        :return: item (string or other python object) if one is immediately
            available, else raise the Empty exception
        """
        return self.fq.get()

    def qsize(self):
        """
        Approximate size of the queue

        Note, qsize() > 0 doesn’t guarantee that a subsequent get() will not
        block, nor will qsize() < maxsize guarantee that put() will not block.

        :return: the approximate size of the queue
        """
        return self.fq.qsize()

    def empty(self):
        """
        Returns True if the queue is empty, False otherwise.

        If empty() returns True it doesn’t guarantee that a subsequent call
        to put() will not block. Similarly, if empty() returns False it
        doesn’t guarantee that a subsequent call to get() will not block.

        :return: True if the queue is empty, False otherwise.
        """
        return self.fq.empty()

    def remove(self):
        self._finalizer()

    def __len__(self):
        return len(self.fq)

    def __str__(self):
        return str(self.fq)

    def __repr__(self):
        return repr(self.fq)


class _PersistentFileQueue(object):

    def __init__(self, directory="", name=None, persistent=False,
                 overwrite=False, id_=0, pickled=True):
        self.pickled = pickled
        self.persistent = persistent
        if name:
            self.name_base = name
        else:
            self.name_base = str(threading.get_ident())
        self.id = str(id_)
        self.overwrite = overwrite
        self.directory = directory.rstrip('/') + '/' if len(directory) else ""
        if not os.path.exists(self.directory) and self.directory:
            os.makedirs(self.directory)
        self.put_queue_name = self._filename('put')
        self.get_queue_name = self._filename('get')
        self.pos_name = self._filename('pos')
        self._update_pos(0)
        self.put_lock = threading.Lock()
        self.get_lock = threading.Lock()
        self.get_queue_length = 0
        self.put_queue_length = 0
        self._puttable = True
        self.iterator = self._iterator()

    def _update_pos(self, i):
        self.get_pos = i
        with open(self.pos_name, 'w') as pn:
            pn.write(str(self.get_pos))

    def put(self, item):
        if not self._puttable:
            raise Empty('Putting to emptied queue is not allowed.')
        with self.put_lock:
            with open(self.put_queue_name, self._file_method('a')) as f:
                if self.pickled:
                    pickle.dump(item, f)
                else:
                    f.write(item + '\n')
            self.put_queue_length += 1

    def get(self):
        with self.get_lock:
            try:
                get_pos, item = next(self.iterator)
                if get_pos == 0:
                    self.get_queue_length = self.put_queue_length
                    self.put_queue_length = 0
                else:
                    self.get_queue_length -= 1
                self._update_pos(get_pos)
                return item
            except StopIteration:
                self._puttable = False
                raise Empty('File queue is empty.')

    def qsize(self):
        return len(self)

    def empty(self):
        return len(self) == 0

    def _filename(self, file_type):
        name = self.directory + file_type + '_thread_' + self.name_base + \
               '_' + self.id + '.queue'
        while os.path.exists(name) and not self.overwrite:
            split_name = name.split('_')
            s_name = split_name[:-1]
            id_ = int(split_name[-1].split('.')[0]) + 1
            s_name.append(str(id_) + '.queue')
            name = '_'.join(s_name)
        _touch(file_name=name, pickled=self.pickled, keep=self.persistent)
        return name

    def _file_method(self, method):
        return _file_method(method, self.pickled)

    def _iterator(self):
        def iterator(put_queue_name, get_queue_name, pos_name, file_method,
                     pickled, put_lock):
            get_pos = 0
            try_again = 0
            while try_again < 2:
                try_again += 1
                with open(get_queue_name, file_method) as f:
                    if pickled:
                        while True:
                            try:
                                unpickled = pickle.load(f)
                            except EOFError:
                                break
                            try_again = 0
                            get_pos += 1
                            yield get_pos, unpickled
                    else:
                        for line in f:
                            get_pos += 1
                            yield get_pos, line.strip('\n')
                            try_again = 0
                with put_lock:
                    os.rename(put_queue_name, get_queue_name)
                    _touch(put_queue_name, pickled=self.pickled)
                    get_pos = 0
            os.remove(put_queue_name)
            os.remove(get_queue_name)
            os.remove(pos_name)

        pos_name = self.pos_name
        put_queue_name = self.put_queue_name
        get_queue_name = self.get_queue_name
        file_method = self._file_method('r')
        pickled = self.pickled
        put_lock = self.put_lock
        return iterator(put_queue_name, get_queue_name, pos_name,
                        file_method, pickled, put_lock)

    def __add__(self, other):
        length = other.get_queue_length
        temp = FileQueue()
        for _ in range(length):
            other.put(other.get())
        for string in other:
            self.put(string)
            temp.put(string)
        for string in temp:
            other.put(string)
        return self

    def __len__(self):
        return self.get_queue_length + self.put_queue_length

    def __str__(self):
        return 'FileQueue for thread {} with length {}.'.format(
            threading.get_ident(), len(self))

    def __repr__(self):
        get_queue_name = os.path.join(os.getcwd(), self.get_queue_name)
        put_queue_name = os.path.join(os.getcwd(), self.put_queue_name)
        return str(self) + 'Files: Read: {} [len {}]; Add: {} [len {}]' \
            ''.format(get_queue_name, self.get_queue_length,
                      put_queue_name, self.put_queue_length)
