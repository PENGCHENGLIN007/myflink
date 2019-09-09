package thread;

public class ThreadTest extends Thread {
	@Override
	public void run(){
		 int count =10;  
	        while(count-->0){
	        	System.out.println(Thread.currentThread().getName()+"  count = " + count);
	        }
		
	}
	
	public static void main(String[] args){
		for(int i=0;i<10;i++){
			new ThreadTest().start();
		}
		
	}

}
