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


def _remove(put_queue_name, get_queue_name, _puttable, persistent, pickled,
            get_pos, initial_get_queue_name, initial_put_queue_name):
    print('#####   deleting filequeue')
    if _puttable:
        if not persistent:
            os.remove(put_queue_name)
            os.remove(get_queue_name)
        else:
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
            if get_queue_name != initial_get_queue_name or \
                    put_queue_name != initial_put_queue_name:
                os.rename(get_queue_name, initial_get_queue_name)
                os.rename(put_queue_name, initial_put_queue_name)


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

        Queue is stored in two files:
        '0_thread_[thread-id or given name]_[id].queue'
        '1_thread_[thread-id or given name]_[id].queue'

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
        self.pickled = pickled
        if name:
            self.name_base = name
        else:
            self.name_base = str(threading.get_ident())
        self.id = str(id_)
        self.overwrite = overwrite
        self.persistent = persistent
        self.directory = directory.rstrip('/') + '/' if len(directory) else ""
        if not os.path.exists(self.directory) and self.directory:
            os.makedirs(self.directory)
        self.put_queue_name = self._filename('0')
        self.get_queue_name = self._filename('1')
        self.initial_put_queue_name = self.put_queue_name
        self.initial_get_queue_name = self.get_queue_name
        self.put_lock = threading.Lock()
        self.get_lock = threading.Lock()
        self.iterator = iter(self._iterator())
        self.get_queue_length = 0
        self.put_queue_length = 0
        self._puttable = True
        self.get_pos = 0
        self._finalizer = weakref.finalize(
            self, _remove, self.put_queue_name, self.get_queue_name,
            self._puttable, self.persistent, self.pickled, self.get_pos,
            self.initial_get_queue_name, self.initial_put_queue_name
        )


    def put(self, item):
        """
        Put item into the queue.

        :param item: string or other Python object to put in queue.
        """
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
        """
        Remove and return an item from the queue.

        Raises Empty when empty.

        :return: item (string or other python object) if one is immediately
            available, else raise the Empty exception
        """
        try:
            return next(self)
        except StopIteration:
            raise Empty('File queue is empty.')

    def qsize(self):
        """
        Approximate size of the queue

        Note, qsize() > 0 doesn’t guarantee that a subsequent get() will not
        block, nor will qsize() < maxsize guarantee that put() will not block.

        :return: the approximate size of the queue
        """
        return len(self)

    def empty(self):
        """
        Returns True if the queue is empty, False otherwise.

        If empty() returns True it doesn’t guarantee that a subsequent call
        to put() will not block. Similarly, if empty() returns False it
        doesn’t guarantee that a subsequent call to get() will not block.

        :return: True if the queue is empty, False otherwise.
        """
        return len(self) == 0

    # def __del__(self):
    #     self._finalizer()
    #
    # def remove(self):
    #     self._finalizer()

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
        try_again = True
        while try_again:
            with self.get_lock, open(self.get_queue_name,
                                     self._file_method('r')) as f:
                if self.pickled:
                    while True:
                        try:
                            self.get_pos += 1
                            unpickled = pickle.load(f)
                            self.get_queue_length -= 1
                            yield unpickled
                        except EOFError:
                            break
                else:
                    for line in f:
                        self.get_pos += 1
                        self.get_queue_length -= 1
                        yield line.strip('\n')
            try_again = self._move_strings_to_get_file()
        os.remove(self.put_queue_name)
        os.remove(self.get_queue_name)
        self._puttable = False
        yield None

    def _move_strings_to_get_file(self):
        try_again = False
        with self.put_lock, self.get_lock:
            if self.get_queue_length != 0:
                if self.persistent:
                    self.get_queue_length = 0
                else:
                    raise FileQueueError('Moving strings to "get" while it is '
                                         'not completely read. pos = {}, '
                                         'get_queue_length = {}'.format(
                        self.get_pos, self.get_queue_length))
            _touch(self.get_queue_name, pickled=self.pickled)
            self.get_queue_length = self.put_queue_length
            self.put_queue_length = 0
            self.get_pos = 0
            get_queue_name_old = self.get_queue_name
            self.get_queue_name = self.put_queue_name
            self.put_queue_name = get_queue_name_old
            try_again = bool(self.get_queue_length)
        return try_again

    def __next__(self):
        next_ = next(self.iterator)
        if next_:
            return next_
        else:
            raise StopIteration

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

    def __iter__(self):
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
