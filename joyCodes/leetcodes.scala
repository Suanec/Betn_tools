class TreeNode(var _value: Int) {
  var value: Int = _value
  var left: TreeNode = null
  var right: TreeNode = null
}
      10
     /  \
    5   -3
   / \    \
  3   2   11
 / \   \
3  -2   1

val root = new TreeNode(10)
root.left = new TreeNode(5)
root.right = new TreeNode(-3)
root.left.left = new TreeNode(3)
root.left.right = new TreeNode(2)
root.right.right = new TreeNode(11)
root.left.left.left = new TreeNode(3)
root.left.left.right = new TreeNode(-2)
root.left.right.right = new TreeNode(1)

object Solution {
    import scala.collection.mutable.HashMap
    def pathSumCore(root: TreeNode): List[scala.collection.mutable.HashMap[Int,Int]] = {
        val sumMap = new scala.collection.mutable.HashMap[Int,Int]
        if(root == null) return List(sumMap)
        sumMap(root.value) = sumMap.getOrElseUpdate(root.value, 0) +1
        
        val leftMapPair = pathSumCore(root.left)
        leftMapPair.head.map{
            pair =>
            sumMap(pair._1 + root.value) = sumMap.getOrElseUpdate(pair._1 + root.value, 0) + pair._2
        }
        
        val rightMapPair = pathSumCore(root.right)
        rightMapPair.head.map{
            pair =>
            sumMap(pair._1 + root.value) = sumMap.getOrElseUpdate(pair._1 + root.value, 0) + pair._2
        }
        List(sumMap) ++ rightMapPair ++ leftMapPair
    }
    def pathSum(root: TreeNode, sum: Int): Int = {
        val rootMap = pathSumCore(root)
        val value = rootMap.flatten.collect{
          case x if(x._1 == sum) => x._2
        }.reduce(_+_)
        value
    }
}
/**
 * Definition for a binary tree node.
 * class TreeNode(var _value: Int) {
 *   var value: Int = _value
 *   var left: TreeNode = null
 *   var right: TreeNode = null
 * }
 */
object Solution {
    def pathSum(root: TreeNode, sum: Int): Int = {
        var m = scala.collection.mutable.Map.empty[Int, Int]
        m.put(0, 1)
        
        def newPathSum(root: TreeNode, curr: Int, sum: Int): Int = {
            var currSum = curr
            if(root == null) return 0
            currSum += root.value
            var res:Int = m.getOrElse(currSum - sum, 0) // check id currsum - target exists in map
            m.put(currSum, m.getOrElse(currSum, 0) + 1)// add curr sum to map, add the 
            res += newPathSum(root.left, currSum, sum) + newPathSum(root.right, currSum, sum)
            m.put(currSum, m.getOrElse(currSum, 1) - 1)// remove curr node from map
            res
        }
        
        newPathSum(root, 0, sum)   
        
    }
}