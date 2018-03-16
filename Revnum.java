import java.util.Scanner;
class Revnum
{
public static void main (String []args)
{
int n=0;
int num=0;
System.out.println("Enter the number: ");
Scanner ss=new Scanner(System.in);
n=ss.nextInt();
while(n!=0)
{
num=num*10;
num=num+n%10;
n=n/10;
}
System.out.print(num);
}
}