import java.util.Scanner;
class Numberrev
{
public static void main (String []args)
{
System.out.println("Enter the number: ");
int val[]=new int[5];
Scanner ss=new Scanner(System.in);
for(int i=0;i<val.length;i++)
{
val[i]=ss.nextInt();
}
for(int i=val.length-1;i>=0;i--)
{
System.out.print(val[i]);
}
}
}
