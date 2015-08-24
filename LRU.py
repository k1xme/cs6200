class PageNode:
    def __init__(self, pageID, value):
        self.pageID = pageID
        self.val = value
        self.prev = None
        self.next = None

    def __repr__(self):
        return str(self.val)

class LRUCache:
    # @param capacity, an integer
    def __init__(self, capacity):
        self.capacity = capacity
        self.pages = {}
        self.leastUsed = None
        self.recentlyUsed = None
        self.count = 0

    # @return an integer
    def get(self, key):
        if key not in self.pages: return -1
        page = self.pages[key]
        if page.prev: page.prev.next = page.next
        page.prev = self.recentlyUsed
        page.next = None
        self.recentlyUsed = page
        return page.val

    # @param key, an integer
    # @param value, an integer
    # @return nothing
    def set(self, key, value):
        if self.count == self.capacity:
            del self.pages[self.leastUsed.pageID]
            self.leastUsed = self.leastUsed.next
            self.count -= 1
        
        if key not in self.pages:
            self.pages[key] = PageNode(key, value)
            self.pages[key].prev = self.recentlyUsed
            self.recentlyUsed = self.pages[key]
            self.count += 1
        else:
            self.pages[key].val = value
            if self.pages[key] is not self.leastUsed:
                self.pages[key].prev.next = self.pages[key].next
                self.pages[key].next = None
        
        self.pages[key].prev = self.recentlyUsed
        if self.count == 1: self.leastUsed = self.pages[key]

lru = LRUCache(2)
lru.set(2,1)
lru.set(1,1)
lru.set(2,3)
print lru.pages, lru.count
lru.set(4,1)
lru.get(1)
lru.get(2)
print lru.pages, lru.count
print "2,",lru.get(2)