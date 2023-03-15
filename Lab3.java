import java.sql.*; 
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;


public class Lab3 implements Callable<long[]>{
    int num = 6;
    int i;
    int iter;
    static Connection connection;
    static String query = "update Authors set copies_sold = copies_sold + 1 where country in (?, ?, ?)";
    static PreparedStatement preparedStmt;

    private void runBookStore1() {
        try {
            for (int i = 0; i < 10; i++) {
                synchronized(preparedStmt) {
                    preparedStmt.setString(1, "Ukraine");
                    preparedStmt.setString(2, "");
                    preparedStmt.setString(3, "");
                    preparedStmt.executeUpdate();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void runBookStore2() {
        try {
            for (int i = 0; i < 11; i++) {
                synchronized(preparedStmt) {
                    preparedStmt.setString(1, "Ukraine");
                    preparedStmt.setString(2, "Great Britain");
                    preparedStmt.setString(3, "USA");
                    preparedStmt.executeUpdate();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void runBookStore3() {
        try {

            for (int i = 0; i < 15; i++) {
                synchronized(preparedStmt) {
                    preparedStmt.setString(1, "");
                    preparedStmt.setString(2, "Great Britain");
                    preparedStmt.setString(3, "");
                    preparedStmt.executeUpdate();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void runBookStore4() {
        try {
            for (int i = 0; i < 10; i++) {
                synchronized(preparedStmt) {
                    preparedStmt.setString(1, "");
                    preparedStmt.setString(2, "Great Britain");
                    preparedStmt.setString(3, "USA");
                    preparedStmt.executeUpdate();
                }
            } 
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void runBookStore5() {
        try {
            for (int i = 0; i < 10; i++) {
                synchronized(preparedStmt) {
                    preparedStmt.setString(1, "");
                    preparedStmt.setString(2, "Great Britain");
                    preparedStmt.setString(3, "USA");
                    preparedStmt.executeUpdate();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private  void runBookStore6() {
        try {
            for (int i = 0; i < 12; i++) {
                synchronized(preparedStmt) {
                    preparedStmt.setString(1, "");
                    preparedStmt.setString(2, "Great Britain");
                    preparedStmt.setString(3, "USA");
                    preparedStmt.executeUpdate();
                }
            }

            synchronized(preparedStmt) {
                preparedStmt.setString(1, "Ukraine");
                preparedStmt.setString(2, "");
                preparedStmt.setString(3, "");
                preparedStmt.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private long run(int iter) {
        long[][] timing = new long[num][2]; 
        
        ExecutorService service = Executors.newFixedThreadPool(num);
        List<Future<long[]>> futureResults = new ArrayList<>();

        for (int j = 1; j <= num; j++) {
            Future<long[]> future = service.submit(new Lab3(iter, j));
            futureResults.add(future);
        }

        long startTime = System.nanoTime();
        for (int j = 0; j < num; j++ ) {
            Future<long[]> futureRes = futureResults.get(j);
            try {
                long[] res = futureRes.get();
                timing[j][0] = res[0];
                timing[j][1] = res[1];
    
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        long estimatedTime = System.nanoTime() - startTime;
        service.shutdown();
        for (int j = 0; j < timing.length; j++) {
            System.out.println("While running book store " + timing[j][0] + " time = " + timing[j][1]);
        }
        System.out.println("-------\n" + iter + ": Summary time taken: " + estimatedTime);
        return estimatedTime;
    }

    public Lab3(int iter, int bookStore) {
        this.i = bookStore;
        this.iter = iter;
    }

    public long[] call() throws InterruptedException, ExecutionException {
        long startTime = System.nanoTime();
        if (i == 1) runBookStore1();
        else if (i == 2) runBookStore2();
        else if (i == 3) runBookStore3();
        else if (i == 4) runBookStore4();
        else if (i == 5) runBookStore5();
        else if (i == 6) runBookStore6();
        long estimatedTime = System.nanoTime() - startTime;
        return new long[] {i, estimatedTime};
    }

    public static void main(String args[]) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Lab3.connection =  DriverManager.getConnection("jdbc:mysql://localhost:6000/liza-db", "dbuser", "dbpassword");
            Lab3.preparedStmt = connection.prepareStatement(Lab3.query);
 
            
            PreparedStatement statement = connection.prepareStatement("update Authors set copies_sold = 0");
            statement.executeUpdate();
            statement.close();

            Lab3 lab3 = new Lab3(0, 0);
            String times = "";

            for (int k = 0; k < 50; k++) {
                long res = lab3.run(k);
                times = times+ res + ", ";
            }

            times = times.substring(0, times.length() - 2);
            System.out.println("------------------------------------\nAll times:\n"+times);


            Lab3.preparedStmt.close();
            Lab3.connection.close();
        }
        catch (Exception exception) {
            System.out.println(exception);
        }
    }
}
