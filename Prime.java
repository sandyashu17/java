import java.util.Scanner;
class Prime
{
public static void main(String []args)
{
int i,flag=0;
System.out.println("Enter the number");
Scanner sc=new Scanner(System.in);
int n=sc.nextInt();
for(i=2;i<=n/2;i++)
{
if(n%i==0)
{
flag=1;
break;
}
}
if(flag==0)
{
System.out.print(n+" is a prime number");
}
else 
{
System.out.print(n+" is not a prime number");
}
}
}
