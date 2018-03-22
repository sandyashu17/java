import java.util.Scanner;
class Calculator
{
public static void main(String args[])
  {
   System.out.println("Enter the Opearion:");
   Scanner sc=new Scanner(System.in);
   int n=sc.nextInt();
   
   for(int i=1;i<=n;i++)
    {
     fact=fact*i;
     }
    System.out.print("Factorial of "+n+" is: "+fact);
   }
}   