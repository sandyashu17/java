import java.util.Scanner;
class Name
{
public static void main(String args[])
{                
int i=1;
System.out.println("Enter the name :");
Scanner ss = new Scanner(System.in);
while(i<6)
{
String S1 = ss.next();
if(i%2!=0)
{
i++;
continue;
}
System.out.println(S1);
i++;
}
}
}