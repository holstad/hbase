package org.apache.hadoop.hbase.regionserver;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;


public class SingleLinkedList<E> implements List<E>{
  protected int size = 0;
  protected Entry<E> head = new Entry(null, null);
  protected Entry<E> tail = new Entry(null, null);
  
  public SingleLinkedList(){}
  
  
  public boolean add(E e){
    Entry<E> newEntry = new Entry<E>(e, null);
    if(head.next == null){
      head.next = newEntry;
    }
    tail.next = newEntry;
    tail = newEntry;
    size++;
    return true;
  }
  
  public int size() {
    return size;
  }

  public void clear(){
    size = 0;
    head = null;
    tail = null;
  }

  private static class Entry<E> {
    E element;
    Entry<E> next;

    Entry(E element, Entry<E> next) {
      this.element = element;
      this.next = next;
    }
  }

  public Iterator<E> iterator() {
    return new ListItr();
  }
  
  private class ListItr implements Iterator<E> {
    private Entry<E> curr = null;
    private Entry<E> prev = null;
    
    ListItr() {
      curr = head;
    }
    
    public boolean hasNext(){
      return curr.next != null;
    }
    
    public E next(){
      prev = curr;
      curr = curr.next;
      return curr.element;
    }
    
    //Since there is only one global prev, it is only possible to do one remove
    //after a next, multiple removes after each other will not work
    public void remove(){
      if(prev != null){
        prev.next = curr.next;
      }
      curr = prev;
    }
    
  }
  
  
  /**
  * Not supported methods
  */
  
  public void add(int index, Object o) {
    throw new UnsupportedOperationException();
  }
  public boolean addAll(Collection c) {
    throw new UnsupportedOperationException();
  }
  public boolean addAll(int index, Collection c) {
    throw new UnsupportedOperationException();
  }  
  
  public boolean contains(Object o) {
    throw new UnsupportedOperationException();
  }
  public boolean containsAll(Collection c) {
    throw new UnsupportedOperationException();
  }
  
  public boolean equals(Object o) {
    throw new UnsupportedOperationException();
  }
  
  public E get(int index) {
    throw new UnsupportedOperationException();
  }
  
  public boolean hashcode() {
    throw new UnsupportedOperationException();
  }
  
  public int indexOf(Object o) {
    throw new UnsupportedOperationException();
  }
  
  public boolean isEmpty() {
    throw new UnsupportedOperationException();
  }
  
  public int lastIndexOf(Object o) {
    throw new UnsupportedOperationException();
  }
  
  public ListIterator listIterator() {
    throw new UnsupportedOperationException();
  }
  public ListIterator listIterator(int index) {
    throw new UnsupportedOperationException();
  }
  
  public E remove(int index) {
    throw new UnsupportedOperationException();
  }
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }
  public boolean removeAll(Collection c) {
    throw new UnsupportedOperationException();
  }
  
  public boolean retainAll(Collection c) {
    throw new UnsupportedOperationException();
  }
  
  public Object set(int index, Object o) {
    throw new UnsupportedOperationException();
  }
  
  public List subList(int fromIndex, int toIndex) {
    throw new UnsupportedOperationException();
  }
  
  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }
  public Object[] toArray(Object[] os) {
    throw new UnsupportedOperationException();
  }
  
}
