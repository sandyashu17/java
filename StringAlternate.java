import java.util.Scanner;
class StringAlternate
{
public static void main (String []args)
{
System.out.println("Enter the string: ");
Scanner ss=new Scanner(System.in);
String s=ss.next();
String[] a=s.split(" ");
for (i=0;i<=
char value[]=.toCharArray();
int flag=1;
for(int i=0;i<=value.length-1;i++)
{
if(value[i]%2==0)
{
flag=1;
}
else 
{
flag=0;
}
if(flag==0)
{
System.out.print(value[i]);
}
}
}
}