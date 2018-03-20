import java.util.Scanner;
class FactorialFunct
{
public void func(int n)
{
int fact=1;
for (int i=1;i<=n;i++)
{
fact=fact*i;
}
System.out.println(fact);
}
public static void main (String []args)
{
System.out.println("Enter the value:");
Scanner sc=new Scanner(System.in);
int n=sc.nextInt();
FactorialFunct ob= new FactorialFunct();
ob.func(n);
}
}