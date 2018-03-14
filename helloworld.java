import java.util.Scanner;
class helloworld
{
public static void main (String []args)
{
System.out.println("Enter the value:");
Scanner sc=new Scanner(System.in);
int n=sc.nextInt();
if(n%3==0 && n%5==0)
{
System.out.println("Hello World");
}
else if (n%3==0)
{
System.out.println("Hello");
}
else
{
System.out.println("World");
}
}
}