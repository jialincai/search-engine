package cis5550.frontend;

import java.util.ArrayList;
import java.util.List;

class TrieNode {
    char val;
    boolean isWord;
    TrieNode[] children = new TrieNode[26];

    public TrieNode() {
    }

    public TrieNode(char c) {
        this.val = c;
    }
}

public class Trie {
    private TrieNode root;

    public Trie() {
        root = new TrieNode();
    }

    public void insert(String word) {
        TrieNode node = root;
        for (char c : word.toCharArray()) {
            if (node.children[c - 'a'] == null) {
                node.children[c - 'a'] = new TrieNode(c);
            }
            node = node.children[c - 'a'];
        }
        node.isWord = true;
    }

    public boolean search(String word) {
        TrieNode node = root;
        for (char c : word.toCharArray()) {
            if (node.children[c - 'a'] == null) {
                return false;
            }
            node = node.children[c - 'a'];
        }
        return node.isWord;
    }

    public boolean startsWith(String prefix) {
        TrieNode node = root;
        for (char c : prefix.toCharArray()) {
            if (node.children[c - 'a'] == null) {
                return false;
            }
            node = node.children[c - 'a'];
        }
        return true;
    }

    public List<String> autoComplete(String prefix) {
        List<String> result = new ArrayList<>();
        TrieNode node = root;
        for (char c : prefix.toCharArray()) {
            if (node.children[c - 'a'] == null) {
                return result;
            }
            node = node.children[c - 'a'];
        }
        dfs(node, prefix, result);
        return result;
    }

    private void dfs(TrieNode node, String word, List<String> result) {
        if (node.isWord) {
            result.add(word);
        }
        for (int i = 0; i < 26; i++) {
            if (node.children[i] != null) {
                dfs(node.children[i], word + node.children[i].val, result);
            }
        }
    }
}
