import pickle
import os
import threading
import weakref

__author__ = 'roelvdberg@gmail.com'


class FileQueueError(Exception):
    pass


class Empty(Exception):
    pass


class FileQueue(object):
    """
    Low memory FIFO* queue that keeps queue on disk instead of in memory.

    * FIFO: First In First Out. When a queue is made persistent and reused,
    this FIFO is broken in a sense that when a queue is deleted (but the
    files still persist) the items still in read are put into add and thus
    these items in front of the queue are put in the back.
    """

    def __init__(self, directory="", name=None, persistent=False,
                 overwrite=False, id_=0, pickled=True):
        """
        Low memory FIFO queue that keeps queue on disk.

        Queue is stored in two files:
        'read_thread_[thread-id or given name]_[id].queue'
        'add_thread_[thread-id or given name]_[id].queue'

        The former (read) is used for reading the queue and gets filled with the
        latter (add) when empty. The latter (add) is used to put new items into
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
        self.add_queue_name = self._filename('add')
        self.read_queue_name = self._filename('read')
        self.add_lock = threading.Lock()
        self.read_lock = threading.Lock()
        self.iterator = iter(self._iterator())
        self._finalizer = weakref.finalize(self, self._remove)
        self.read_queue_length = 0
        self.add_queue_length = 0
        self._puttable = True

    def put(self, item):
        """
        Put item into the queue. Only eats strings.

        :param item: string to put in queue.
        """
        if not self._puttable:
            raise Empty('Putting to emptied queue is not allowed.')
        with self.add_lock:
            with open(self.add_queue_name, self._method('a')) as f:
                if self.pickled:
                    pickle.dump(item, f)
                else:
                    f.write(item + '\n')
            self.add_queue_length += 1

    def get(self):
        """
        Remove and return an item from the queue.

        Raises Empty when empty.

        :return: item (string) if one is immediately available, else raise the
            Empty exception
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

    def remove(self):
        """
        Removes queue files when persistent or puts them in a reusable state.

        Files are made reusable by moving all items from read-file to add-file.
        """
        self._finalizer()

    def _filename(self, file_type):
        name = self.directory + file_type + '_thread_' + self.name_base + \
               '_'+ self.id + '.queue'
        while os.path.exists(name) and not self.overwrite:
            split_name = name.split('_')
            s_name = split_name[:-1]
            id_ = int(split_name[-1].split('.')[0]) + 1
            s_name.append(str(id_) + '.queue')
            name = '_'.join(s_name)
        self._touch(file_name=name, keep=self.persistent)
        return name

    def _touch(self, file_name, keep=False):
        with open(file_name, self._method('a') if keep else self._method('w')):
            os.utime(file_name)

    def _iterator(self):
        try_again = True
        while try_again:
            with self.read_lock, \
                    open(self.read_queue_name, self._method('r')) as f:
                if self.pickled:
                    while True:
                        try:
                            yield pickle.load(f)
                        except EOFError:
                            break
                else:
                    for line in f:
                        self.read_queue_length -= 1
                        yield line.strip('\n')
            try_again = self._move_strings_to_read()
        os.remove(self.add_queue_name)
        os.remove(self.read_queue_name)
        self._puttable = False
        yield None

    def _move_strings_to_read(self):
        try_again = False
        with self.add_lock, self.read_lock:
            if self.read_queue_length != 0:
                if self.persistent:
                    self.read_queue_length = 0
                else:
                    raise FileQueueError('Moving strings to "read" while it is '
                                         'not completely read.')
            self._touch(self.read_queue_name)
            added_length = self._move(from_=self.add_queue_name,
                                                 to=self.read_queue_name)
            self.read_queue_length += added_length
            try_again = bool(added_length)
            self._touch(self.add_queue_name)
            self.add_queue_length = 0
        return try_again

    def _move(self, from_, to):
        i = 0
        with open(from_, self._method('r')) as from_file, \
                open(to, self._method('a')) as to_file:
            if self.pickled:
                while True:
                    try:
                        pickle.dump(pickle.load(from_file), to_file)
                        i += 1
                    except EOFError:
                        break
            else:
                for line in from_file:
                    to_file.write(line)
                    i += 1
        return i

    def _remove(self):
        if self._puttable:
            if not self.persistent:
                os.remove(self.add_queue_name)
                os.remove(self.read_queue_name)
            else:
                with open(self.add_queue_name, self._method('a')) as add_queue:
                    for _ in range(self.read_queue_length):
                        if self.pickled:
                            pickle.dump(self.get(), add_queue)
                        else:
                            add_queue.write(self.get() + '\n')
                self._touch(self.read_queue_name)

    def _method(self, method):
        return method + 'b' if self.pickled else method

    def __next__(self):
        next_ = next(self.iterator)
        if next_:
            return next_
        else:
            raise StopIteration

    def __add__(self, other):
        length = other.read_queue_length
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
        return self.read_queue_length + self.add_queue_length

    def __str__(self):
        return 'FileQueue for thread {} with length {}.'.format(
            threading.get_ident(), len(self))

    def __repr__(self):
        return str(self) + 'Files: Read: {} [len {}]; Add: {} [len {}]' \
                           ''.format(os.path.join(os.getcwd(),
                                                  self.read_queue_name),
                                     self.read_queue_length,
                                     os.path.join(os.getcwd(),
                                                  self.add_queue_name),
                                     self.add_queue_length)
