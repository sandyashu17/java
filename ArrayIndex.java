import java.util.Scanner;
public class ArrayIndex
{
public static void main(String[] args)
{
int i=0, f=0;
System.out.println("Enter the array number: ");
int val[]=new int[4];
Scanner ss=new Scanner(System.in);
for(i=0;i<val.length;i++)
{
val[i]=ss.nextInt();
}
System.out.println("Enter a number: ");
Scanner sc=new Scanner(System.in);
int n=sc.nextInt();
for( int j=0;j<val.length;j++)
{
if(n==val[j])
{
System.out.println(j);
}
else
{	
f=0;
}
}
if(f==0)
{
System.out.println("not in array");
}
}
}

