/**
 * @author smi1e
 * Date 2019/10/24 23:08
 * Description
 */
public class BSTTest {
    static class BST {
        int data;
        BST lchild, rchild;
    }

    static int BSTInsert(BST T, int k) {
        if (T == null) {
            T = new BST();
            T.data = k;
            T.lchild = T.rchild = null;
            return 1;
        } else if (T.data == k) {
            return 0;
        } else if (k < T.data) {
            return BSTInsert(T.lchild, k);
        } else {
            return BSTInsert(T.rchild, k);
        }
    }

    static void dfs(BST T) {
        if (T == null) {
            return;
            /* code */
        }
        dfs(T.lchild);
        System.out.println(T.data);
        dfs(T.rchild);
    }

    public static void main(String[] args) {
        BST root = null;
        BSTInsert(root, 14);
        BSTInsert(root, 1);
        BSTInsert(root, 15);
        BSTInsert(root, 2);
        BSTInsert(root, 0);
        dfs(root);
    }

}
