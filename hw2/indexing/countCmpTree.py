class TreeNode:
    def __init__(self, x):
        self.val = x
        self.left = None
        self.right = None

class Solution:
    # @param {TreeNode} root
    # @return {integer}
    def countNodes(self, root):
        if not root: return 0
        h = 1        
        stack = [(root, 1 + 2**h)]
        count = 0

        while stack:
            v, approxNodes = stack.pop()
            print v.val, approxNodes
            if v.right:
                h += 1
                approxNodes += 2**h
                stack.append((v.left, approxNodes - 2))
                stack.append((v.right, approxNodes))
            elif v.left:
                return approxNodes - 1
            count = approxNodes - 2
        return count

s = Solution()
r = TreeNode(1)
r.left = TreeNode(0)
r.right = TreeNode(3)
r.left.left = TreeNode(4)
r.left.right = TreeNode(5)

print s.countNodes(r)