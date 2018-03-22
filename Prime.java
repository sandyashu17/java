import java.util.Scanner;
class Prime
{
 public static void main(String args[])
  {
    System.out.print("Enter a number:");
    Scanner sc=new Scanner(System.in);
    int n=sc.nextInt();
    if(n==n/n && n==n/1)
    {
     System.out.println(n+" is a prime number");
    }
    else
    {
    System.out.println(n+" is not a prime number"); 
    } 
  }
}
