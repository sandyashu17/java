import java.util.Scanner;
class FibonacciLimit
{
public static void main (String []args)
{
int n1=0, n3;
System.out.println("Enter the second number:");
Scanner sc=new Scanner(System.in);
int n2=sc.nextInt();
System.out.println("Enter the last number:");
Scanner ss=new Scanner(System.in);
int n=ss.nextInt();
System.out.print(n1+" "+n2);
for (int i=1;i<n;i++)
{
n3=n1+n2;
System.out.print(" "+n3);
n1=n2;
n2=n3;
if(n3==n)
{
break;
}
}
}
}